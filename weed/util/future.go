package util

type Future struct {
	doneCh chan error
	err    error
}

func (g *Future) Complete() *Future {
	if g.doneCh != nil {
		<-g.doneCh
	}
	return g
}

func (g *Future) Error() error {
	return g.err
}

func (g *Future) Done() *Future {
	if g.doneCh != nil {
		close(g.doneCh)
	}
	return g
}

func (g *Future) SetError(e error) {
	g.err = e
}

func NewFuture() *Future {
	return &Future{doneCh: make(chan error)}
}
