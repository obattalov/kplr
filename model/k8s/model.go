package k8s

import (
	"fmt"

	"github.com/kplr-io/kplr/model"
)

type (
	// Descriptor contains model description used in k8s environment
	Descriptor struct {
		egMeta model.Meta
	}

	// k8s Event Group Meta can be used for usability
	EgMeta struct {
		SrcId     string
		PodId     string
		CntId     string
		Namespace string
	}
)

const (
	EGMETA_SRCID_FLD       = 0
	EGMETA_PODID_FLD       = 1
	EGMETA_CONTAINERID_FLD = 2
	EGMETA_NAMESPACE_FLD   = 3

	EGMETA_FIELDS = 4
)

var MetaDesc = model.Meta{
	model.FTString, // Source Id
	model.FTString, // Pod Id
	model.FTString, // Container Id
	model.FTString, // Namespace
}

func NewDescriptor() *Descriptor {
	d := new(Descriptor)
	d.egMeta = MetaDesc
	return d
}

func (d *Descriptor) EventGroupMeta() model.Meta {
	return d.egMeta
}

func (d *Descriptor) GetJournalId(evGrpMeta model.Event) string {
	return evGrpMeta[EGMETA_SRCID_FLD].(string)
}

func (egm *EgMeta) ToEvent(evGrpMeta model.Event) {
	evGrpMeta[EGMETA_SRCID_FLD] = egm.SrcId
	evGrpMeta[EGMETA_PODID_FLD] = egm.PodId
	evGrpMeta[EGMETA_CONTAINERID_FLD] = egm.CntId
	evGrpMeta[EGMETA_NAMESPACE_FLD] = egm.Namespace
}

func (egm *EgMeta) Event() model.Event {
	ev := make(model.Event, EGMETA_FIELDS)
	egm.ToEvent(ev)
	return ev
}

func (egm *EgMeta) String() string {
	return fmt.Sprint("{SrcId=", egm.SrcId, ", PodId=", egm.PodId, ", CntId=", egm.CntId, ", Namespace=", egm.Namespace, "}")
}
