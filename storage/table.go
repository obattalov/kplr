package storage

import (
	"path/filepath"

	"github.com/kplr-io/kplr/model"
)

type (
	Table interface {
		Upsert(rec model.Tags) error
		Search(query model.Tags) []model.Tags
	}
)

func f() {
	filepath.Match()
}
