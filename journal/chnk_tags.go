package journal

import (
	"fmt"

	"github.com/kplr-io/journal"
)

type (
	// str_set is just a set of strings defined via map
	str_set map[string]bool

	// int_set is just a set of integers
	int_set map[uint32]bool

	// a chunk descriptor
	chunk_desc struct {
		// set of tags met in in the chunk
		Tags str_set

		// a last known record for the chunk
		LastRec journal.RecordId
	}

	// chnk_tags keeps information about tags stored in a journal chunk
	chnk_tags struct {
		// tags maps a Tag list with a set of chunks that keep the information
		// about the chunks.
		tags map[string]int_set

		// chnks defines a set of chunk desriptors mapped by the chunk id
		chnks map[uint32]*chunk_desc
	}
)

func new_chnk_tags() *chnk_tags {
	ct := new(chnk_tags)
	ct.tags = make(map[string]int_set)
	ct.chnks = make(map[uint32]*chunk_desc)
	return ct
}

func new_chnk_desc() *chunk_desc {
	cd := new(chunk_desc)
	cd.Tags = make(str_set)
	return cd
}

func (cd *chunk_desc) String() string {
	return fmt.Sprint("{tags=", cd.Tags, ", lastRec=", cd.LastRec, "}")
}

// on_chnk_tags is called when tags are added to a chunk. Returns whether the
// tags have just been added for the chunk, or were there before
func (ct *chnk_tags) on_chnk_tags(tags string, lastRec journal.RecordId) bool {
	cs, ok := ct.tags[tags]
	if !ok {
		cs = make(int_set)
		ct.tags[tags] = cs
	}
	cs[lastRec.ChunkId] = true

	cd, ok := ct.chnks[lastRec.ChunkId]
	added := true
	if !ok {
		cd = new_chnk_desc()
		ct.chnks[lastRec.ChunkId] = cd
	} else {
		_, ok = cd.Tags[tags]
		added = !ok
	}

	cd.LastRec.ChunkId = lastRec.ChunkId
	cd.LastRec.Offset = lastRec.Offset
	cd.Tags[tags] = true
	return added
}

// on_chnk_delete is called when a chunk is deleted
func (ct *chnk_tags) on_chnk_delete(chnkId uint32) {
	cd, ok := ct.chnks[chnkId]
	if !ok {
		return
	}
	delete(ct.chnks, chnkId)

	for tags, _ := range cd.Tags {
		cs, ok := ct.tags[tags]
		if ok {
			delete(cs, chnkId)
			if len(cs) == 0 {
				delete(ct.tags, tags)
			}
		}
	}
}

func (ct *chnk_tags) get_tags_as_slice() []string {
	res := make([]string, 0, len(ct.tags))
	for tags := range ct.tags {
		res = append(res, tags)
	}
	return res
}

// get_tags returns a map of tags to the journal Id
func (ct *chnk_tags) get_tags(jid string) map[string]string {
	res := make(map[string]string, len(ct.tags))
	for tags, _ := range ct.tags {
		res[tags] = jid
	}
	return res
}

// is_consistent returns true if the data is consistent with chunks status
func (ct *chnk_tags) is_consistent(chunks []journal.Chunk) bool {
	for _, c := range chunks {
		lr := c.GetLastRecordId()
		cd, ok := ct.chnks[lr.ChunkId]
		if !ok {
			return false
		}

		if cd.LastRec.Offset != lr.Offset {
			return false
		}
	}
	return true
}

func (ct *chnk_tags) on_chnks_load(chnks map[uint32]*chunk_desc) {
	ct.tags = make(map[string]int_set)
	ct.chnks = chnks
	for chnk, desc := range chnks {
		for tags, _ := range desc.Tags {
			cs, ok := ct.tags[tags]
			if !ok {
				cs = make(int_set)
				ct.tags[tags] = cs
			}
			cs[chnk] = true
		}
	}
}
