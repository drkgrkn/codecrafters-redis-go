package protocol

type Command interface {
	ProcessRequest(req string) (string, error)
}

type Echo struct{}

func (ec Echo) ProcessRequest(req string) (string, error) {
	return req, nil
}
