package main

import (
	"fmt"
	"net/http"
	"os"
	"time"

	parser "github.com/hanchuanchuan/bingo2sql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	ini "gopkg.in/ini.v1"
)

var parserProcess map[string]*parser.MyBinlogParser

func init() {

}

var (
	logHeader     = `${time_rfc3339} ${prefix} ${level} ${short_file} ${line} `
	requestHeader = `${time_rfc3339} ${remote_ip} ${method} ${uri} ${status} ${error} ${latency_human}` + "\n"
)

var (
	runServer  = flagBoolean("server", "s", false, "以服务方式运行")
	configFile = flag.StringP("config", "c", "config.ini", "bingo2sql config file")

	host     = flag.StringP("host", "h", "", "host")
	port     = flag.IntP("port", "P", 3306, "host")
	user     = flag.StringP("user", "u", "", "user")
	password = flag.StringP("password", "p", "", "password")

	startFile = flag.String("start-file", "", "start-file")
	stopFile  = flag.String("stop-file", "", "stop-file")

	startTime = flag.String("start-time", "", "start-time")
	stopTime  = flag.String("stop-time", "", "stop-time")

	startPosition = flag.Int("start-pos", 0, "start-pos")
	stopPosition  = flag.Int("stop-pos", 0, "stop-pos")

	flashback = flagBoolean("flashback", "f", false, "逆向语句")

	parseDDL = flagBoolean("ddl", "", false, "解析DDL语句(仅正向SQL)")

	databases = flag.StringP("databases", "d", "", "数据库列表,多个时以逗号分隔")
	tables    = flag.StringP("tables", "t", "", "表名,如果数据库为多个,则需指名表前缀,多个时以逗号分隔")
	sqlType   = flag.String("type", "insert,delete,update", "解析的语句类型")

	maxRows = flag.Int("max", 100000, "解析的最大行数,设置为0则不限制")

	output = flag.String("output", "", "output file")

	debug = flagBoolean("debug", "", false, "调试模式,输出详细日志.sets log level to debug")

	stopNever = flagBoolean("stop-never", "", false, "持续解析binlog")

	removePrimary = flagBoolean("no-primary-key", "K", false, "对INSERT语句去除主键. 可选. 默认False")
)

func main() {

	flag.Parse()

	parserProcess = make(map[string]*parser.MyBinlogParser)

	if *configFile == "" {
		flag.Usage()
		return
	}

	// 以服务方式运行
	if *runServer {
		startServer()
	} else {

		if *debug {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.ErrorLevel)
		}

		// output := zerolog.ConsoleWriter{
		// 	Out:        os.Stdout,
		// 	TimeFormat: "2006-01-02 15:04:05"}
		// log.Logger = zerolog.New(output).With().Timestamp().Logger()

		// 以独立工具运行
		runParse()
	}
}

// runParse 执行binlog解析
func runParse() {
	cfg := &parser.BinlogParserConfig{
		Host:     *host,
		Port:     uint16(*port),
		User:     *user,
		Password: *password,

		StartFile:     *startFile,
		StopFile:      *stopFile,
		StartPosition: *startPosition,
		StopPosition:  *stopPosition,

		StartTime: *startTime,
		StopTime:  *stopTime,

		Flashback: *flashback,

		ParseDDL: *parseDDL,

		Databases: *databases,
		Tables:    *tables,
		SqlType:   *sqlType,
		MaxRows:   *maxRows,

		OutputFileStr: *output,

		RemovePrimary: *removePrimary,
	}

	if p, err := parser.NewBinlogParser(cfg); err != nil {
		log.Error("binlog解析操作失败")
		log.Error(err)
		return
	} else {
		cfg.BeginTime = time.Now().Unix()
		err = p.Parser()
		if err != nil {
			log.Error("binlog解析操作失败")
			log.Error(err)
			return
		}
	}
}

// startServer 启动binlog解析服务
func startServer() {
	fmt.Println(*runServer)
	cnf, err := ini.Load(*configFile)
	if err != nil {
		fmt.Println(fmt.Sprintf(`read config file %s error: %s`, *configFile, err.Error()))
		return
	}

	logDir := cnf.Section("Bingo").Key("log").String()
	httpLogDir := cnf.Section("Bingo").Key("httplog").String()
	// level := cnf.Section("Bingo").Key("logLevel").String()

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

	// lvl, _ := zerolog.ParseLevel(level)
	// zerolog.SetGlobalLevel(lvl)

	// log.Logger = log.With().Caller().Logger().Output(
	// 	zerolog.ConsoleWriter{Out: elog, NoColor: true})

	// log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})

	//new echo
	e := echo.New()
	e.Server.WriteTimeout = time.Duration(30) * time.Second

	// Middleware
	e.Use(middleware.LoggerWithConfig(
		middleware.LoggerConfig{Output: httplog, Format: requestHeader}))
	e.Use(middleware.Recover())

	log.Info(`parse binlog tool is started`)

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

	p, err := parser.NewBinlogParser(cfg)
	if err != nil {
		log.Error("binlog解析操作失败")
		log.Error(err)
		out := map[string]string{"error": err.Error()}
		return c.JSON(http.StatusOK, out)
	}

	if err := recover(); err != nil {

		if e, ok := err.(error); ok {
			log.Error("binlog解析操作失败")
			log.Error(err)
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

	log.Infof("下载路径: %s", path)

	path = "../files/" + path

	return c.Attachment(path, c.Param("name"))

	// _, err := os.Stat(path)
	// if err == nil || os.IsExist(err) {
	// } else {

	// }

	// return c.Inline(path, c.Param("name"))

	// return c.File(path)
}

func flagBoolean(name string, shorthand string, defaultVal bool, usage string) *bool {
	if defaultVal == false {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
		return flag.BoolP(name, shorthand, defaultVal, usage)
	}
	return flag.BoolP(name, shorthand, defaultVal, usage)
}
