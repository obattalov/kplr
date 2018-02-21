// index package contains interfaces that are used for data indexing in the
// general data querying process
package index

import (
	"github.com/kplr-io/kplr/model"
)

type (
	// RecordsBatch describes a batch of records added to a journal
	RecordsBatch struct {
		// TGroupId how the batch was tagged
		TGroupId int64

		// the last record in the batch
		LastRecord model.JournalRecord
	}

	TagsIndexer interface {
		// UpsertTags creates new record, or returns already exisiting one for
		// the tag-line provided
		UpsertTags(tl model.TagLine) model.Tags

		// OnRecords is called by writer to notify the indexer about a write
		// of batch of records
		OnRecords(rb *RecordsBatch) error
	}
)
