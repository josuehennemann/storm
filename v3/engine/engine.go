package engine

type Bucket interface {
	Next() (Record, error)
	Schema() (*Schema, error)
}

type Engine interface {
	Begin(writable bool) (Transaction, error)
}

type Transaction interface {
	Rollback() error
	Commit() error
	Insert(r Record, path ...string) (key []byte, err error)
	Bucket(path ...string) (Bucket, error)
}

type Pipe func(Bucket) (Bucket, error)

type Pipeline []Pipe

func (pl Pipeline) Run(b Bucket) (Bucket, error) {
	var err error

	for _, p := range pl {
		b, err = p(b)
		if err != nil {
			return nil, err
		}
	}

	return b, nil
}
