/**
 * REST API router
 * Rosbit Xu
 */
package main

import (
	"fmt"
	"go-search/conf"
	"go-search/indexer"
	"go-search/rest"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	helper "github.com/rosbit/http-helper"
)

// 设置路由，进入服务状态
func StartService() error {
	initIndexers()

	api := helper.NewHelper()

	_ = api.GET("/schema/:index", rest.ShowSchema)
	_ = api.POST("/schema/:index", rest.CreateSchema)
	_ = api.DELETE("/schema/:index", rest.DeleteSchema)
	_ = api.PUT("/schema/:index/:newIndex", rest.RenameSchema)
	_ = api.PUT("/doc/:index", rest.IndexDoc)
	_ = api.PUT("/docs/:index", rest.IndexDocs)
	_ = api.PUT("/update/:index", rest.UpdateDoc)
	_ = api.DELETE("/doc/:index", rest.DeleteDoc)
	_ = api.DELETE("/docs/:index", rest.DeleteDocs)
	_ = api.GET("/search/:index", rest.Search)

	// health check
	_ = api.GET("/health", func(c *helper.Context) {
		_ = c.String(http.StatusOK, "OK\n")
	})

	serviceConf := conf.ServiceConf
	listenParam := fmt.Sprintf("%s:%d", serviceConf.ListenHost, serviceConf.ListenPort)
	log.Printf("I am listening at %s...\n", listenParam)
	return http.ListenAndServe(listenParam, api)
}

func initIndexers() {
	indexer.StartIndexers(conf.ServiceConf.WorkerNum)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	go func() {
		for range c {
			log.Println("I will exit in a while")
			indexer.StopIndexers(conf.ServiceConf.WorkerNum)
			os.Exit(0)
		}
	}()
}
