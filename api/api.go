package api

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jrivets/log4g"
	"github.com/kplr-io/container"
	"github.com/kplr-io/kplr"
	"github.com/kplr-io/kplr/cursor"
	"github.com/kplr-io/kplr/journal"
	"github.com/kplr-io/kplr/model/index"
	"github.com/kplr-io/kplr/model/kql"
	"github.com/teris-io/shortid"
)

type (
	RestApiConfig interface {
		GetHttpAddress() string
		GetHttpShtdwnTOSec() int
		IsHttpDebugMode() bool

		// GetHttpsCertFile in case of TLS returns non-empty filename with TLS cert
		GetHttpsCertFile() string
		// GetHttpsKeyFile in case of TLS returns non-empty filename with private key
		GetHttpsKeyFile() string
	}

	RestApi struct {
		logger         log4g.Logger
		ge             *gin.Engine
		srv            *http.Server
		TIndx          index.TagsIndexer     `inject:"tIndexer"`
		Config         RestApiConfig         `inject:"restApiConfig"`
		CursorProvider cursor.CursorProvider `inject:""`
		JrnlCtrlr      journal.Controller    `inject:""`
		MCtx           context.Context       `inject:"mainCtx"`

		rdsCnt    int32
		lock      sync.Mutex
		cursors   *container.Lru
		ctx       context.Context
		ctxCancel context.CancelFunc
	}

	api_error struct {
		err_tp int
		msg    string
	}

	error_resp struct {
		Status       int    `json:"status"`
		ErrorMessage string `json:"error"`
	}

	// cur_desc a structure which contains information about persisted cursors
	cur_desc struct {
		cur cursor.Cursor

		createdAt   time.Time
		lastTouched time.Time
		lastKQL     string
	}
)

const (
	ERR_INVALID_CNT_TYPE = 1
	ERR_INVALID_PARAM    = 2
	ERR_NOT_FOUND        = 3

	CURSOR_TTL_SEC = 300
	MAX_CUR_SRCS   = 50
)

func NewError(tp int, msg string) error {
	return &api_error{tp, msg}
}

func (ae *api_error) Error() string {
	return ae.msg
}

func (ae *api_error) get_error_resp() error_resp {
	switch ae.err_tp {
	case ERR_INVALID_CNT_TYPE:
		return error_resp{http.StatusBadRequest, ae.msg}
	case ERR_INVALID_PARAM:
		return error_resp{http.StatusBadRequest, ae.msg}
	case ERR_NOT_FOUND:
		return error_resp{http.StatusNotFound, ae.msg}
	}
	return error_resp{http.StatusInternalServerError, ae.msg}
}

func NewRestApi() *RestApi {
	ra := new(RestApi)
	ra.logger = log4g.GetLogger("kplr.RestApi")
	ra.cursors = container.NewLru(math.MaxInt64, CURSOR_TTL_SEC*time.Second, ra.onCursorDeleted)
	return ra
}

func (ra *RestApi) DiPhase() int {
	return 1
}

func (ra *RestApi) DiInit() error {
	ra.logger.Info("Initializing. IsHttpDebugMode=", ra.Config.IsHttpDebugMode(), " listenOn=", ra.Config.GetHttpAddress(), ", shutdown timeout sec=", ra.Config.GetHttpShtdwnTOSec())
	if !ra.Config.IsHttpDebugMode() {
		gin.SetMode(gin.ReleaseMode)
	}

	ra.ge = gin.New()
	if ra.Config.IsHttpDebugMode() {
		ra.logger.Info("Gin logger and gin.debug is enabled. You can set up DEBUG mode for the ", ra.logger.GetName(), " group to obtain requests dumps and more logs for the API group.")
		log4g.SetLogLevel(ra.logger.GetName(), log4g.DEBUG)
		ra.ge.Use(gin.Logger())
	}

	// To log requests if DEBUG is enabled
	ra.ge.Use(ra.PrintRequest)
	// Recovery middleware recovers from any panics and writes a 500 if there was one.
	ra.ge.Use(gin.Recovery())

	ra.logger.Info("Constructing ReST API")

	// The ping returns pong and URI of the ping, how we see it.
	ra.ge.GET("/ping", ra.h_GET_ping)
	ra.ge.GET("/logs", ra.h_GET_logs)
	ra.ge.POST("/cursors", ra.h_POST_cursors)
	ra.ge.GET("/cursors/:curId", ra.h_GET_cursors_curId)
	ra.ge.GET("/cursors/:curId/logs", ra.h_GET_cursors_curId_logs)
	ra.ge.GET("/journals", ra.h_GET_journals)
	ra.ge.GET("/journals/:jId", ra.h_GET_journals_jId)

	ra.run()

	ra.ctx, ra.ctxCancel = context.WithCancel(context.Background())

	go func() {
		defer ra.logger.Info("Sweeper goroutine is over.")
	L1:
		for {
			select {
			case <-ra.ctx.Done():
				break L1
			case <-time.After((CURSOR_TTL_SEC / 2) * time.Second):
				ra.lock.Lock()
				ra.cursors.SweepByTime()
				ra.lock.Unlock()
			}
		}
	}()

	return nil
}

func (ra *RestApi) DiShutdown() {
	ra.ctxCancel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(ra.Config.GetHttpShtdwnTOSec())*time.Second)
	defer cancel()

	if err := ra.srv.Shutdown(ctx); err != nil {
		ra.logger.Error("Server Shutdown err=", err)
	}
	ra.logger.Info("Shutdown.")
}

func (ra *RestApi) run() {
	ra.srv = &http.Server{
		Addr:    ra.Config.GetHttpAddress(),
		Handler: ra.ge,
	}

	go func() {
		ra.logger.Info("Running listener on ", ra.Config.GetHttpAddress())
		defer ra.logger.Info("Stopping listener")

		certFN := ra.Config.GetHttpsCertFile()
		keyFN := ra.Config.GetHttpsKeyFile()
		if certFN != "" || keyFN != "" {
			ra.logger.Info("Serves HTTPS connections: cert location ", certFN, ", private key at ", keyFN)
			if err := ra.srv.ListenAndServeTLS(certFN, keyFN); err != nil {
				ra.logger.Warn("Got the error from the server HTTPS listener err=", err)
			}
		} else {
			ra.logger.Info("Serves HTTP connections")
			if err := ra.srv.ListenAndServe(); err != nil {
				ra.logger.Warn("Got the error from the server HTTP listener err=", err)
			}
		}
	}()
}

// ============================== Filters ===================================
func (ra *RestApi) PrintRequest(c *gin.Context) {
	if ra.logger.GetLevel() >= log4g.DEBUG {
		r, _ := httputil.DumpRequest(c.Request, true)
		ra.logger.Debug("\n>>> REQUEST\n", string(r), "\n<<< REQUEST")
	}
	c.Next()
}

// ============================== Handlers ===================================
// GET /ping
func (ra *RestApi) h_GET_ping(c *gin.Context) {
	c.String(http.StatusOK, "pong URL conversion is "+composeURI(c.Request, ""))
}

// GET /logs
func (ra *RestApi) h_GET_logs(c *gin.Context) {
	q := c.Request.URL.Query()
	kqlTxt, err := ra.parseRequest(c, q, "tail")
	if ra.errorResponse(c, wrapErrorInvalidParam(err)) {
		ra.logger.Warn("GET /logs invalid reqeust err=", err)
		return
	}

	blocked := parseBoolean(q, "blocked", true)
	ra.logger.Debug("GET /logs kql=", kqlTxt, ", blocked=", blocked)

	cur, qry, err := ra.newCursorByQuery(kqlTxt)
	if ra.errorResponse(c, wrapErrorInvalidParam(err)) {
		return
	}

	rdr := cur.GetReader(qry.Limit(), blocked)
	ra.sendData(c, rdr, blocked)
	cur.Close()
}

// POST /cursors
func (ra *RestApi) h_POST_cursors(c *gin.Context) {
	curId, err := shortid.Generate()
	if ra.errorResponse(c, err) {
		return
	}

	q := c.Request.URL.Query()
	kqlTxt, err := ra.parseRequest(c, q, "head")
	if ra.errorResponse(c, wrapErrorInvalidParam(err)) {
		ra.logger.Warn("POST /cursors invalid reqeust err=", err)
		return
	}

	cur, qry, err := ra.newCursorByQuery(kqlTxt)
	if ra.errorResponse(c, wrapErrorInvalidParam(err)) {
		return
	}
	cd := ra.newCurDesc(cur)
	cd.setKQL(kqlTxt)

	ra.logger.Info("New cursor desc ", cd)

	w := c.Writer
	uri := composeURI(c.Request, curId)
	w.Header().Set("Location", uri)
	c.Status(http.StatusCreated)

	rdr := cur.GetReader(qry.Limit(), false)
	ra.sendData(c, rdr, false)
	ra.putCursorDesc(curId, cd)
}

// GET /cursors/:curId
func (ra *RestApi) h_GET_cursors_curId(c *gin.Context) {
	curId := c.Param("curId")
	cd := ra.getCursorDesc(curId)
	if cd == nil {
		ra.errorResponse(c, NewError(ERR_NOT_FOUND, "The cursors id="+curId+" is not known"))
		return
	}

	defer ra.putCursorDesc(curId, cd)

	ra.logger.Debug("Get cursor state curId=", curId, " ", cd)
	c.JSON(http.StatusOK, toCurDescDO(cd, curId))
}

// GET /cursors/:curId/logs
func (ra *RestApi) h_GET_cursors_curId_logs(c *gin.Context) {
	curId := c.Param("curId")
	cd := ra.getCursorDesc(curId)
	if cd == nil {
		ra.errorResponse(c, NewError(ERR_NOT_FOUND, "The cursors id="+curId+" is not known"))
		return
	}

	defer ra.putCursorDesc(curId, cd)

	q := c.Request.URL.Query()
	kqlTxt, err := ra.parseRequest(c, q, "")
	if ra.errorResponse(c, wrapErrorInvalidParam(err)) {
		ra.logger.Warn("GET /cursors/", curId, "/logs invalid reqeust err=", err)
		return
	}

	qry, err := ra.applyQueryToCursor(kqlTxt, cd.cur)
	if ra.errorResponse(c, err) {
		return
	}
	cd.setKQL(kqlTxt)

	rdr := cd.cur.GetReader(qry.Limit(), false)
	ra.sendData(c, rdr, false)
}

// GET /journals
func (ra *RestApi) h_GET_journals(c *gin.Context) {
	jrnls := ra.JrnlCtrlr.GetJournals()

	var page PageDo
	page.Data = jrnls
	page.Count = len(jrnls)
	page.Total = len(jrnls)
	page.Offset = 0

	c.JSON(http.StatusOK, &page)
}

// GET /journals/:jId
func (ra *RestApi) h_GET_journals_jId(c *gin.Context) {
	jId := c.Param("jId")
	ji, err := ra.JrnlCtrlr.GetJournalInfo(jId)
	if ra.errorResponse(c, err) {
		return
	}
	c.JSON(http.StatusOK, ji)
}

func (ra *RestApi) newCurDesc(cur cursor.Cursor) *cur_desc {
	cd := new(cur_desc)
	cd.createdAt = time.Now()
	cd.cur = cur
	cd.lastTouched = cd.createdAt
	return cd
}

func (ra *RestApi) getCursorDesc(curId string) *cur_desc {
	ra.lock.Lock()
	defer ra.lock.Unlock()

	val := ra.cursors.Peek(curId)
	if val == nil {
		return nil
	}
	cd := val.Val().(*cur_desc)
	ra.cursors.DeleteNoCallback(curId)
	cd.lastTouched = time.Now()
	return cd
}

func (ra *RestApi) putCursorDesc(curId string, cd *cur_desc) {
	ra.lock.Lock()
	ra.cursors.Put(curId, cd, 1)
	ra.lock.Unlock()
}

func (ra *RestApi) onCursorDeleted(k, v interface{}) {
	ra.logger.Info("Cursor ", k, " is deleted")
	cd := v.(*cur_desc)
	cd.cur.Close()
}

// parseRequest parses HTTP request. It expects query specified by url-encoded
// or via body string. For query the following params are supported:
// where - optional. Allows to specify complex where conditions
// limit - optional, default -1. Allows to specify limit
// offset - optional, how many records must be skipped. Depends on request
// position - optional, specifies position where the cursor should start from.
//     allowed values head, tail and base64 encoded position, if known.
//
// Body can be specified too. If provided the body context is returned, all
// other params are ignored
func (ra *RestApi) parseRequest(c *gin.Context, q url.Values, defPos string) (string, error) {
	var qbuf bytes.Buffer
	bdy := c.Request.Body
	if bdy != nil {
		n, err := qbuf.ReadFrom(bdy)
		if n > 0 {
			return qbuf.String(), err
		}
	}

	offset := int64(0)
	limit := int64(-1)
	addCond := false
	where := false
	position := defPos

	ra.logger.Debug("request params ", q)
	for k, v := range q {
		if len(v) < 1 {
			continue
		}

		var err error

		switch strings.ToLower(k) {
		case "limit":
			limit, err = strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return "", NewError(ERR_INVALID_PARAM, fmt.Sprint("'limit' must be an integer (negative value means no limit), but got ", v[0]))
			}
		case "offset":
			offset, err = strconv.ParseInt(v[0], 10, 64)
			if err != nil {
				return "", NewError(ERR_INVALID_PARAM, fmt.Sprint("'offset' must be an integer, but got ", v[0]))
			}
		case "where":
			qbuf.Reset()
			qbuf.WriteString("SELECT WHERE ")
			qbuf.WriteString(v[0])
			where = true
		case "blocked":
			// ignore blocked
			continue
		case "position":
			position = v[0]
		default:
			if where {
				break
			}
			if addCond {
				qbuf.WriteString(" AND ")
			} else {
				qbuf.WriteString("SELECT WHERE ")
			}
			addCond = true
			qbuf.WriteString(k)
			qbuf.WriteString("=")
			qbuf.WriteString(v[0])
		}
	}

	if !addCond && !where {
		qbuf.WriteString("SELECT ")
	}

	// just to make it parses properly
	if position != "" {
		qbuf.WriteString(" POSITION '")
		qbuf.WriteString(position)
		qbuf.WriteString("'")
	}
	qbuf.WriteString(" OFFSET ")
	qbuf.WriteString(strconv.FormatInt(offset, 10))
	qbuf.WriteString(" LIMIT ")
	qbuf.WriteString(strconv.FormatInt(limit, 10))

	return qbuf.String(), nil
}

func (ra *RestApi) newCursorByQuery(kqlTxt string) (cursor.Cursor, *kql.Query, error) {
	qry, err := kql.Compile(kqlTxt, ra.TIndx)
	if err != nil {
		return nil, nil, NewError(ERR_INVALID_PARAM, fmt.Sprint("Parsing error of automatically generated query='", kqlTxt, "', check the query syntax (escape params needed?), parser says: ", err))
	}

	jrnls := qry.Sources()
	if len(jrnls) > MAX_CUR_SRCS {
		return nil, nil, NewError(ERR_INVALID_PARAM, fmt.Sprint("Number of sources for the log query execution exceds maximum allowed value ", MAX_CUR_SRCS, ", please make your query more specific"))
	}

	if len(jrnls) == 0 {
		return nil, nil, NewError(ERR_INVALID_PARAM, fmt.Sprint("Could not define sources for the query=", kqlTxt, ", the error=", err))
	}

	id := atomic.AddInt32(&ra.rdsCnt, 1)
	cur, err := ra.CursorProvider.NewCursor(&cursor.CursorSettings{
		CursorId:  strconv.Itoa(int(id)),
		Sources:   jrnls,
		Formatter: qry.GetFormatter(),
	})
	if err != nil {
		return cur, qry, err
	}

	// qry.Position() never returns ""...
	posDO := qry.Position()
	pos, err := curPosDOToCurPos(posDO)
	if err != nil {
		return nil, nil, NewError(ERR_INVALID_PARAM, fmt.Sprint("Could not recognize position=", qry.Position(), " the error=", err))
	}

	cur.SetFilter(qry.GetFilterF())
	if posDO == "tail" {
		skip := int64(1)
		if qry.Offset() > 0 {
			skip = qry.Offset()
		}
		cur.SkipFromTail(skip)
	} else {
		cur.SetPosition(pos)
		cur.Offset(qry.Offset())
	}

	return cur, qry, nil
}

func (ra *RestApi) applyQueryToCursor(kqlTxt string, cur cursor.Cursor) (*kql.Query, error) {
	qry, err := kql.Compile(kqlTxt, ra.TIndx)
	if err != nil {
		return nil, NewError(ERR_INVALID_PARAM, fmt.Sprint("Parsing error of automatically generated query='", kqlTxt, "', check the query syntax (escape params needed?), parser says: ", err))
	}

	cur.SetFilter(qry.GetFilterF())
	offs := qry.Offset()
	if qry.QSel.Position != nil {
		posDO := qry.Position()
		pos, err := curPosDOToCurPos(posDO)
		if err != nil {
			return nil, NewError(ERR_INVALID_PARAM, fmt.Sprint("Could not recognize position=", qry.Position(), " the error=", err))
		}

		if posDO == "tail" {
			skip := int64(1)
			if offs > 0 {
				skip = qry.Offset()
			}
			cur.SkipFromTail(skip)
			offs = 0
		} else {
			cur.SetPosition(pos)
		}
	}
	cur.Offset(offs)

	return qry, nil
}

// sendData copies data from the reader to the context writer. If blocked is
// true, that means reader can block Read operation and will not return io.EOF,
// this case we set up the connection notification to stop the copying process
// in case of the connection is closed to release resources properly.
func (ra *RestApi) sendData(c *gin.Context, rdr io.ReadCloser, blocked bool) {
	w := c.Writer
	id := atomic.AddInt32(&ra.rdsCnt, 1)

	ra.logger.Debug("sendData(): id=", id, ", blocked=", blocked)
	if blocked {
		notify := w.CloseNotify()
		clsd := make(chan bool)
		defer close(clsd)

		go func() {
		L1:
			for {
				select {
				case <-time.After(500 * time.Millisecond):
					w.Flush()
				case <-notify:
					ra.logger.Debug("sendData(): <-notify, id=", id)
					break L1
				case <-clsd:
					ra.logger.Debug("sendData(): <-clsd, id=", id)
					break L1
				case <-ra.MCtx.Done():
					ra.logger.Debug("sendData(): <-ra.MCtx.Done(), id=", id)
					break L1
				}
			}
			rdr.Close()
		}()
	}

	w.Header().Set("Content-Disposition", "attachment; filename=logs.txt")
	io.Copy(w, rdr)
	rdr.Close()
	ra.logger.Debug("sendData(): over id=", id)
}

// =============================== cur_desc ==================================
func (cd *cur_desc) setKQL(kql string) {
	cd.lastKQL = kql
	cd.lastTouched = time.Now()
}

func (cd *cur_desc) String() string {
	return fmt.Sprint("{curId=", cd.cur.Id(), ", created=", cd.createdAt, ", lastTouched=", cd.lastTouched,
		", lastKql='", cd.lastKQL, "'}")
}

// ================================ Misc =====================================
func parseBoolean(q url.Values, paramName string, defVal bool) bool {
	val, ok := q[paramName]
	if !ok || len(val) < 1 {
		return defVal
	}

	res, err := strconv.ParseBool(val[0])
	if err != nil {
		return defVal
	}

	return res
}

func bindAppJson(c *gin.Context, inf interface{}) error {
	return c.BindJSON(inf)
}

func reqOp(c *gin.Context) string {
	return fmt.Sprint(c.Request.Method, " ", c.Request.URL)
}

func wrapErrorInvalidParam(err error) error {
	if err == nil {
		return nil
	}
	return NewError(ERR_INVALID_PARAM, err.Error())
}

func (ra *RestApi) errorResponse(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}

	ae, ok := err.(*api_error)
	if ok {
		er := ae.get_error_resp()
		c.JSON(er.Status, &er)
		return true
	}

	if err == kplr.ErrNotFound {
		c.Status(http.StatusNotFound)
		return true
	}

	ra.logger.Warn("Bad request err=", err)
	c.JSON(http.StatusInternalServerError, &error_resp{http.StatusInternalServerError, err.Error()})
	return true
}

func composeURIWithPath(r *http.Request, pth, id string) string {
	return resolveScheme(r) + "://" + path.Join(resolveHost(r), pth, id)
}

func composeURI(r *http.Request, id string) string {
	var resURL string
	if r.URL.IsAbs() {
		resURL = path.Join(r.URL.String(), id)
	} else {
		resURL = resolveScheme(r) + "://" + path.Join(resolveHost(r), r.URL.String(), id)
	}
	return resURL
}

func resolveScheme(r *http.Request) string {
	switch {
	case r.Header.Get("X-Forwarded-Proto") == "https":
		return "https"
	case r.URL.Scheme == "https":
		return "https"
	case r.TLS != nil:
		return "https"
	case strings.HasPrefix(r.Proto, "HTTPS"):
		return "https"
	default:
		return "http"
	}
}

func resolveHost(r *http.Request) (host string) {
	switch {
	case r.Header.Get("X-Forwarded-For") != "":
		return r.Header.Get("X-Forwarded-For")
	case r.Header.Get("X-Host") != "":
		return r.Header.Get("X-Host")
	case r.Host != "":
		return r.Host
	case r.URL.Host != "":
		return r.URL.Host
	default:
		return ""
	}
}
