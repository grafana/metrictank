package query

type QueryGenerator interface {
	GetPattern() string
	Start()
}
