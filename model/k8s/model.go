package k8s

import (
	"github.com/kplr-io/kplr/model"
)

type (
	// Descriptor contains model description used in k8s environment
	Descriptor struct {
		egMeta model.Meta
	}
)

const (
	EGMETA_SRCID_FLD = 0
)

func NewDescriptor() *Descriptor {
	d := new(Descriptor)
	d.egMeta = model.Meta{
		model.FTString, // Source Id
	}
	return d
}

func (d *Descriptor) EventGroupMeta() model.Meta {
	return d.egMeta
}

func (d *Descriptor) GetJournalId(evGrpMeta model.Event) string {
	return evGrpMeta[EGMETA_SRCID_FLD].(string)
}
