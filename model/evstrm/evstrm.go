package evstrm

import (
	"io"

	"github.com/kplr-io/kplr/model"
)

type (
	IteratorPos interface{}

	// Iterator is an interface which provides methods for iterating over a
	// journal (see journal.Iterator), or group of journals (see Mixer).
	Iterator interface {
		io.Closer

		// End() Returns whether the end is reached. Moving Forward it means
		// that the current position is after the last record in the set, and in
		// case of moving Backward it means that current position is less than
		// first record in the collection
		//
		// If the function returns true, consequentive calls of Get() and Next()
		// will probably not to change the value
		End() bool

		// Get() reads event or returns an error if the operation failed. It returns
		// io.EOF if the end is reached (End() == true)
		Get(le *model.LogEvent) error

		// Next switches to the next record either before or after the current one
		// depending on direction.
		Next()

		// Backward sets the direction to backward if the provided parameter
		// is true, or forward, if it is false.
		Backward(bkwrd bool)

		// GetIteratorPos returns position of the record returned by Get(). The position
		// could make sense only when Get() returns valid value and not an error.
		// Semantic of the IteratorPos depends on the underlying implementation
		// and for different storages can be implemented by different objects
		GetIteratorPos() IteratorPos
	}
)
