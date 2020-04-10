package out

import "github.com/grafana/metrictank/schema"

type multiErr struct {
	errors []error
}

func (me multiErr) Return() error {
	if len(me.errors) != 0 {
		return me
	}
	return nil
}

func (me multiErr) Error() string {
	var str string
	for i, e := range me.errors {
		if i > 0 {
			str += "\n"
		}
		str += e.Error()
	}
	return str
}

type FanOut struct {
	outs []Out
}

func (f FanOut) Close() error {
	var retErr multiErr
	for _, o := range f.outs {
		err := o.Close()
		if err != nil {
			retErr.errors = append(retErr.errors, err)
		}
	}
	return retErr.Return()
}

func (f FanOut) Flush(metrics []*schema.MetricData) error {
	var retErr multiErr
	for _, o := range f.outs {
		err := o.Flush(metrics)
		if err != nil {
			retErr.errors = append(retErr.errors, err)
		}
	}
	return retErr.Return()
}
