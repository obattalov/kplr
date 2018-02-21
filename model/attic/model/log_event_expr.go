package model

type (
	// FilterF returns true, if the event must be filtered (disregarded), or false
	// if it is not.
	FilterF func(ev *LogEvent) bool

	// ChkF defines a function which evaluates a condition over provided
	// LogEvent value and it returns the evanluation result
	ChkF func(le *LogEvent) bool

	IndexCond struct {
		Tag   string
		Op    string
		Value string
	}

	// ExprDesc - an expressing descriptor, which contains the expression
	// function valueation (ChkF) and the index descriptor, if any. The index
	// descriptor allows to use the index for selecting records for the evaluation
	ExprDesc struct {
		// The index can be used to select records by the condition. The field
		// makes sense only for Priorities 1 and 2.
		Index IndexCond

		// Function to calcualte the expression
		Expr ChkF

		// Priority:
		// 0 - disregard, always true or not relevant at all
		// 1 - indexed by exact value
		// 2 - indexed by greater/less condition or interval
		// 3 - cannot be indexed, all records must be checked
		Priority int
	}
)

func (exp *ExprDesc) ChooseBetter(exp2 *ExprDesc) {
	if exp.Priority > exp2.Priority {
		exp.Index = exp2.Index
		exp.Priority = exp2.Priority
	}
}

func (exp *ExprDesc) ChooseWorse(exp2 *ExprDesc) {
	if exp.Priority < exp2.Priority {
		exp.Index = exp2.Index
		exp.Priority = exp2.Priority
	}
}
