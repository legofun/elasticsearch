package elasticsearch

type esError struct {
	err error
}

func (e esError) Error() string {
	return "<elasticsearch error> "+e.err.Error()
}

func newEsError(err error) error {
	return esError{err}
}
