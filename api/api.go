package api

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"path"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jrivets/log4g"
	"github.com/kplr-io/kplr/cursor"
	"github.com/kplr-io/kplr/model/query"
)

type (
	RestApiConfig interface {
		GetHttpAddress() string
		GetHttpShtdwnTOSec() int
		IsHttpDebugMode() bool
	}

	RestApi struct {
		logger         log4g.Logger
		ge             *gin.Engine
		srv            *http.Server
		Config         RestApiConfig         `inject:"restApiConfig"`
		CursorProvider cursor.CursorProvider `inject:""`
	}

	api_error struct {
		err_tp int
		msg    string
	}

	error_resp struct {
		Status       int    `json:"status"`
		ErrorMessage string `json:"error"`
	}
)

const (
	ERR_INVALID_CNT_TYPE = 1
	ERR_INVALID_PARAM    = 2
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
	}
	return error_resp{http.StatusInternalServerError, ae.msg}
}

func NewRestApi() *RestApi {
	ra := new(RestApi)
	ra.logger = log4g.GetLogger("kplr.RestApi")
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

	// select allows to download logs by the request
	ra.ge.POST("/select", ra.h_POST_select)

	ra.run()

	return nil
}

func (ra *RestApi) DiShutdown() {
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
		// service connections
		if err := ra.srv.ListenAndServe(); err != nil {
			ra.logger.Warn("Got the error from the server listener err=", err)
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
func (ra *RestApi) h_GET_ping(c *gin.Context) {
	ra.logger.Debug("GET /ping")
	c.String(http.StatusOK, "pong URL conversion is "+composeURI(c.Request, ""))
}

func (ra *RestApi) h_POST_select(c *gin.Context) {
	ra.logger.Debug("POST /select")

	var sq SelectQuery
	if ra.errorResponse(c, bindAppJson(c, &sq)) {
		return
	}
	kqry := sq.applyParams()
	ra.logger.Debug("Got query \"", kqry, "\" for ", &sq)

	q, err := query.NewQuery(kqry)
	if ra.errorResponse(c, wrapErrorInvalidParam(err)) {
		return
	}

	cur, err := ra.CursorProvider.NewCursor(q)
	if ra.errorResponse(c, wrapErrorInvalidParam(err)) {
		return
	}

	w := c.Writer
	w.Header().Set("Content-Disposition", "attachment; filename=logs.tar.gz")
	io.Copy(w, cur.GetRecords(q.Limit()))
}

func bindAppJson(c *gin.Context, inf interface{}) error {
	ct := c.ContentType()
	if ct != "application/json" {
		return NewError(ERR_INVALID_CNT_TYPE, fmt.Sprint("Expected content type for the request is 'application/json', but really is ", ct))
	}
	return c.Bind(inf)
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
