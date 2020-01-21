package ingest

type MetricsResponse struct {
	Invalid          int
	Published        int
	ValidationErrors map[string]ValidationError
}

func NewMetricsResponse() MetricsResponse {
	return MetricsResponse{
		ValidationErrors: make(map[string]ValidationError),
	}
}

type ValidationError struct {
	Count      int
	ExampleIds []int
}

// AddInvalid updates the counts and makes sure there's up to 10 examples per error
func (m *MetricsResponse) AddInvalid(err error, index int) {
	key := err.Error()
	m.Invalid++
	vErr := m.ValidationErrors[key]
	vErr.Count++

	if vErr.Count < 10 {
		vErr.ExampleIds = append(vErr.ExampleIds, index)
	}

	m.ValidationErrors[key] = vErr
}
