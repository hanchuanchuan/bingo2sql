/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/hanchuanchuan/bingo2sql/parse"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/siddontang/go/log"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

// rootCmd represents the base command when called without any subcommands
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "以服务方式运行",
	Long:  `用于解析mysql binlog,支持正向,逆向. 可本地或远程解析.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		startServer()

	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	flag := serverCmd.Flags()

	flag.StringVarP(&cfgFile, "config", "c", "config.ini", "以服务方式启动时需指定配置文件")
}

func startServer() {

	viper := viper.New()
	viper.SetConfigFile(cfgFile)
	viper.SetConfigType("ini")

	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error: %s", err.Error())
		return
	}

	// logDir := cnf.Section("Bingo").Key("log").String()
	// httpLogDir := cnf.Section("Bingo").Key("httplog").String()
	// level := cnf.Section("Bingo").Key("logLevel").String()

	logDir := viper.GetString("Bingo.log")
	httpLogDir := viper.GetString("Bingo.httplog")

	//echo's output log file
	elog, err := os.OpenFile(logDir, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(fmt.Sprintf(`open echo log file %s error: %s`, logDir, err.Error()))
		return
	}
	defer elog.Close()

	httplog, err := os.OpenFile(httpLogDir, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Println(fmt.Sprintf(`open echo log file %s error: %s`, httpLogDir, err.Error()))
		return
	}
	defer httplog.Close()

	// // 初始化Router
	// r := mux.NewRouter()
	// // // 静态文件路由
	// // r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir(dir))))
	// // 普通路由
	// r.HandleFunc("/", HomeHandler)

	// r.Use(TestMiddleware)
	// http.ListenAndServe(":3000", r)

	// lvl, _ := zerolog.ParseLevel(level)
	// zerolog.SetGlobalLevel(lvl)

	// log.Logger = log.With().Caller().Logger().Output(
	// 	zerolog.ConsoleWriter{Out: elog, NoColor: true})

	// log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	//new echo
	router := echo.New()
	router.Server.WriteTimeout = time.Duration(30) * time.Second

	// Middleware
	router.Use(middleware.Logger())
	router.Use(middleware.Recover())

	// Middleware
	// router.Use(middleware.LoggerWithConfig(
	// 	middleware.LoggerConfig{Output: httplog, Format: requestHeader}))
	// router.Use(middleware.Recover())

	log.Info(`parse binlog tool is started`)
	group := router.Group("/binlog")

	group.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!\n")
	})

	// 发起binlog解析请求
	group.POST("/parse", parse.ParseBinlog)

	// 解析进程的进度
	group.GET("/parse/:id", parse.GetParseInfo)

	// 获取所有解析
	group.GET("/parse", parse.GetAllParse)

	// 停止解析操作
	group.DELETE("/parse/:id", parse.ParseBinlogStop)

	// 下载解析生成的文件
	group.GET("/parse/download/:id", parse.Download)

	// router.Logger.Fatal(router.Start(addr))
	router.Logger.Fatal(router.Start(":8077"))
}
