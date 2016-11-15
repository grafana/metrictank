package middleware

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/rs/cors"
	"gopkg.in/macaron.v1"
)

type Context struct {
	*macaron.Context
	OrgId int
}

func OrgMiddleware() macaron.Handler {
	return func(c *macaron.Context) {
		org, err := getOrg(c.Req.Request)
		if err != nil {
			c.PlainText(400, []byte(err.Error()))
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

func RequireOrg() macaron.Handler {
	return func(c *Context) {
		if c.OrgId == 0 {
			c.PlainText(401, []byte("x-org-id header missing."))
		}
	}
}

func CorsHandler() macaron.Handler {
	c := cors.New(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "PUT", "POST", "DELETE"},
		AllowCredentials: true,
	})
	return c.HandlerFunc
}
