package handlers

import (
	"examples/fiber-rabbitmq-mysql-example/service"

	"github.com/gofiber/fiber/v2"
)

func Greet(svc *service.GreetSvc) fiber.Handler {
	return func(c *fiber.Ctx) error {
		name := c.Query("name", "Unknow")

		err := svc.Greet(c.Context(), name)
		if err != nil {
			return err
		}

		c.Status(200)
		c.WriteString("ok")
		return nil
	}
}
