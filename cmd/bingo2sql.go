package main

import (
	"fmt"

	parser "github.com/hanchuanchuan/bingo2sql"

	// "github.com/jinzhu/gorm"
	"flag"
	"net/http"
	"os"

	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	log2 "github.com/siddontang/go-log/log"
	ini "gopkg.in/ini.v1"

	// "strings"
	"time"
)

var parserProcess map[string]*parser.MyBinlogParser

func init() {

}

var (
	logHeader     = `${time_rfc3339} ${prefix} ${level} ${short_file} ${line} `
	requestHeader = `${time_rfc3339} ${remote_ip} ${method} ${uri} ${status} ${error} ${latency_human}` + "\n"
)

func init() {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05"
}

func main() {

	parserProcess = make(map[string]*parser.MyBinlogParser)

	//config file
	var fc string
	flag.StringVar(&fc, "c", "config.ini", "myRobot config file")
	flag.Parse()

	if fc == "" {
		flag.Usage()
		return
	}

	//config file parse
	// cnf, err := ini.Load(path.Join(dir, fc))
	cnf, err := ini.Load(fc)
	if err != nil {
		fmt.Println(fmt.Sprintf(`read config file %s error: %s`, fc, err.Error()))
		return
	}

	// addr := cnf.Section("Bingo").Key("addr").String()
	writeTimeout, _ := cnf.Section("Bingo").Key("writeTimeout").Int()
	logDir := cnf.Section("Bingo").Key("log").String()
	httpLogDir := cnf.Section("Bingo").Key("httplog").String()
	level := cnf.Section("Bingo").Key("logLevel").String()

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

	lvl, _ := zerolog.ParseLevel(level)
	zerolog.SetGlobalLevel(lvl)

	log.Logger = log.With().Caller().Logger().Output(
		zerolog.ConsoleWriter{Out: elog, NoColor: true})

	// log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	h, err := log2.NewStreamHandler(elog)
	if err != nil {
		fmt.Println(fmt.Sprintf(`open echo log file %s error: %s`, logDir, err.Error()))
		return
	}

	l := log2.NewDefault(h)
	log2.SetDefaultLogger(l)
	log2.SetLevelByName(level)
	// log2.SetLevel(log2.LevelInfo)

	//new echo
	e := echo.New()
	e.Server.WriteTimeout = time.Duration(writeTimeout) * time.Second

	// Middleware
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{Output: httplog, Format: requestHeader}))
	e.Use(middleware.Recover())

	log.Info().Msg(`parse binlog tool is started`)

	e.GET("/", func(c echo.Context) error {
		return c.String(http.StatusOK, "Hello, World!\n")
	})

	e.POST("/go/mysql/binlog/parse", parseBinlog)

	e.POST("/go/mysql/binlog/parse_stop/:id", parseBinlogStop)

	e.GET("/go/download/files/:name", download)

	// e.POST("/go/mysql/binlog/parse_work/:work_id/:db_id", parseBinlogWork)

	// e.Logger.Fatal(e.Start(addr))
	e.Logger.Fatal(e.Start(":8077"))

}

func parseBinlog(c echo.Context) error {
	cfg := new(parser.BinlogParserConfig)

	if err := c.Bind(cfg); err != nil {
		return err
	}

	fmt.Println(cfg)
	fmt.Printf("%#v\n", cfg)

	if cfg.InsID == 0 {
		r := map[string]string{"error": "请指定数据库地址"}
		return c.JSON(http.StatusOK, r)
	}

	// cfg.SetRemoteDB()

	p := parser.NewBinlogParser(cfg)

	if err := recover(); err != nil {

		if e, ok := err.(error); ok {
			log.Error().Err(e).Msg("binlog解析操作失败")
			out := map[string]string{"error": e.Error()}
			return c.JSON(http.StatusOK, out)
		} else {
			out := map[string]string{"error": "未知的错误"}
			return c.JSON(http.StatusOK, out)
		}
	} else {
		cfg.BeginTime = time.Now().Unix()

		i := cfg.Id()
		parserProcess[i] = p
		go func() {
			defer delete(parserProcess, i)
			p.Parser()
		}()

		r := map[string]string{"id": cfg.Id()}
		return c.JSON(http.StatusOK, r)
	}
}

// func parseBinlogWork(c echo.Context) error {

// 	m := echo.Map{}
// 	if err := c.Bind(&m); err != nil {
// 		return err
// 	}

// 	socket_user, _ := m["socket_user"]

// 	work_id := c.Param("work_id")
// 	str_db_id := c.Param("db_id")

// 	db_id, _ := strconv.Atoi(str_db_id)

// 	r := make(map[string]string)

// 	err := parser.ProcessWork(work_id, db_id, socket_user.(string))
// 	if err != nil {
// 		r["error"] = err.Error()
// 	} else {
// 		r["ok"] = "1"
// 	}
// 	return c.JSON(http.StatusOK, r)
// }

func parseBinlogStop(c echo.Context) error {
	id := c.Param("id")
	r := make(map[string]string)

	if len(id) == 0 {
		r["error"] = "无效参数!"
		return c.JSON(http.StatusOK, r)
	}

	// fmt.Println("当前解析进程数量: ", len(parserProcess))
	log.Print("当前解析进程数量: ", len(parserProcess))

	defer delete(parserProcess, id)

	if p, ok := parserProcess[id]; ok {
		p.Stop()

		r["ok"] = "1"
		return c.JSON(http.StatusOK, r)
	} else {
		r["ok"] = "2"
		return c.JSON(http.StatusOK, r)
	}
}

func download(c echo.Context) error {
	path := c.Param("name")

	log.Info().Str("download_path", path).Msg("下载路径")

	path = "../files/" + path

	return c.Attachment(path, c.Param("name"))

	// _, err := os.Stat(path)
	// if err == nil || os.IsExist(err) {
	// } else {

	// }

	// return c.Inline(path, c.Param("name"))

	// return c.File(path)
}
