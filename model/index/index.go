// index package contains interfaces that are used for data indexing in the
// general data querying process
package index

import (
	"github.com/kplr-io/journal"
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

	ChunkDescs map[uint32]*ChunkDesc

	// ChunkDesc struct contains a chunk descriptor for a data
	ChunkDesc struct {
		// LastRecord contains last point where the data is met
		LastRecord journal.RecordId
	}

	// TagsDesc struct contains descriptor for the set of tags, it contains
	// tags and the list of journals where the tags met
	TagsDesc struct {
		Tags     *model.Tags
		Journals map[string]ChunkDescs
	}

	// TagsIndexerVisitor for visiting all known tags. The function should
	// return whether visiting should be continued or not. The result doesn't
	// make sense for last tags record
	//
	// td param is for READ ONLY and MUST NOT be modified by the visitor
	TagsIndexerVisitor func(td *TagsDesc) bool

	TagsIndexerConfig interface {
		GetJournalDir() string
	}

	TagsIndexer interface {
		// UpsertTags creates new record, or returns already exisiting one for
		// the tag-line provided
		UpsertTags(tl model.TagLine) (*model.Tags, error)

		// OnRecords is called by writer to notify the indexer about a write
		// of batch of records
		OnRecords(rb *RecordsBatch) error

		// OnDelete is called when the journal chunk is deleted
		OnDelete(jrnl string, cid uint32) error

		// Get list of all known journals
		GetAllJournals() []string

		GetTagsDesc(tgid int64) *TagsDesc

		// Visit, walks through all known records and gives them to the visitor
		Visit(visitor TagsIndexerVisitor)
	}
)
