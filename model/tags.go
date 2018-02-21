package model

type (
	// TagLine contains a list of tags in a form |tag1=val1|tag2=val2|...| the tags
	// are sorted alphabetically in ascending order
	TagLine string

	// Tags storage where the key is the tag name and it is holded by its value
	TagMap map[string]string

	// An immutable structure which holds a reference to the TagMap
	Tags struct {
		gId int64
		tl  TagLine
		tm  TagMap
	}
)
