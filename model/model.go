// model package contains data types are used for data representations. The
// package defines also some common types which are used for various data
// processing procedures
package model

import (
	"github.com/kplr-io/journal"
)

type (
	JournalRecord struct {
		Journal  string
		RecordId journal.RecordId
	}
)
