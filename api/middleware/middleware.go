package middleware

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/Unknwon/macaron"
)

type Context struct {
	*macaron.Context
	OrgId int
}

func OrgMiddleware() macaron.Handler {
	return func(c *macaron.Context) {
		org, err := getOrg(c.Req.Request)
		if err != nil {
			c.JSON(400, err.Error())
			return
		}
		ctx := &Context{
			Context: c,
			OrgId:   org,
		}
		c.Map(ctx)
	}
}

func getOrg(req *http.Request) (int, error) {
	orgStr := req.Header.Get("x-org-id")
	if orgStr == "" {
		return 0, nil
	}
	org, err := strconv.Atoi(orgStr)
	if err != nil {
		return 0, errors.New("bad org-id")
	}
	return org, nil
}

func CorsHandler() macaron.Handler {
	return func(c *macaron.Context) {
		if c.Req.Method == "OPTIONS" {
			// nothing to do, CORS headers already sent
			c.Header().Set("Access-Control-Allow-Origin", "*")
			c.Header().Add("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
		}
	}
}
