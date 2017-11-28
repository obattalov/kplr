package model

type (
	// The Descriptor interface provides an abstraction for a kplr environment.
	// It provides some helper methods for describing some specific environment,
	// for instance, it returns Meta object for event group headers, which
	// could contain different fields in different environments. Or the Descriptor
	// implementation can return journal id calculated by the meta header etc.
	Descriptor interface {
		// EventGroupMeta returns Meta object for a group of objects
		EventGroupMeta() Meta

		// GetJournalId returns journal id by event group meta info
		GetJournalId(evGrpMeta Event) string
	}
)
