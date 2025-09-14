package pipeline

type Fetcher interface {
}

type Pipeline struct {
	fetcher *Fetcher
}

func NewPipeline(fetcher Fetcher) *Pipeline {
	return &Pipeline{fetcher: &fetcher}
}

func (pipeline *Pipeline) Run() error {
	return nil
}
