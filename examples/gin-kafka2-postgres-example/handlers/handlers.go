package handlers

import (
	"examples/gin-kafka2-postgres-example/service"

	"github.com/gin-gonic/gin"
)

func Say(svc *service.SaySvc) gin.HandlerFunc {
	return func(c *gin.Context) {
		name := c.DefaultQuery("name", "Unknown")

		err := svc.Say(c, name)
		if err != nil {
			c.String(500, err.Error())
			return
		}

		c.String(200, "ok")
	}
}
