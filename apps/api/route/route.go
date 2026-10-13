
package route

import "github.com/gin-gonic/gin"

func RegisterRoutes(router *gin.Engine) {
	v1 := router.Group("/v1")

	RegisterHealthRoutes(v1)

}
