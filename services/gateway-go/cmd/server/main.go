package main

import (
	"os"

	httpapi "github.com/Haimbeau1o/trustops-content-quality-platform/services/gateway-go/internal/http"
	"github.com/cloudwego/hertz/pkg/app/server"
)

func main() {
	addr := os.Getenv("GATEWAY_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	h := server.Default(server.WithHostPorts(addr))
	httpapi.RegisterRoutes(h)
	h.Spin()
}
