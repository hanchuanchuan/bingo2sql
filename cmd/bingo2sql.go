package main

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	parser "github.com/hanchuanchuan/bingo2sql"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// var parserProcess map[string]*parser.MyBinlogParser
var parserProcess sync.Map

type ParseInfo struct {
	ID        string `json:"id"`
	ParseRows int    `json:"parse_rows"`
	Percent   int    `json:"percent"`
}

var (
	logHeader     = `${time_rfc3339} ${prefix} ${level} ${short_file} ${line} `
	requestHeader = `${time_rfc3339} ${remote_ip} ${method} ${uri} ${status} ${error} ${latency_human}` + "\n"
	flag          = pflag.NewFlagSet("bingo2sql", pflag.ExitOnError)
)

var (
	runServer  = flagBoolean("server", "s", false, "启动API服务")
	configFile = flag.StringP("config", "c", "config.ini", "以服务方式启动时可指定配置文件")

	host     = flag.StringP("host", "h", "", "数据库地址")
	port     = flag.IntP("port", "P", 3306, "端口号")
	user     = flag.StringP("user", "u", "", "用户名")
	password = flag.StringP("password", "p", "", "密码")

	startFile = flag.String("start-file", "", "起始binlog文件")
	stopFile  = flag.String("stop-file", "", "结束binlog文件")

	startPosition = flag.Int("start-pos", 0, "起始位置")
	stopPosition  = flag.Int("stop-pos", 0, "结束位置")

	startTime = flag.String("start-time", "", "开始时间")
	stopTime  = flag.String("stop-time", "", "结束时间")

	databases = flag.StringP("databases", "d", "", "数据库列表,多个时以逗号分隔")
	tables    = flag.StringP("tables", "t", "", "表名,如果数据库为多个,则需指名表前缀,多个时以逗号分隔")

	threadID = flag.Uint64P("connection-id", "C", 0, "指定线程ID")

	flashback = flagBoolean("flashback", "B", false, "逆向语句")

	parseDDL = flagBoolean("ddl", "", false, "解析DDL语句(仅正向SQL)")

	sqlType = flag.String("type", "insert,delete,update", "解析的语句类型")

	maxRows = flag.Int("max", 100000, "解析的最大行数,设置为0则不限制")

	output = flag.StringP("output", "o", "", "输出到指定文件")

	gtid = flag.StringP("gtid", "g", "", "GTID范围.格式为uuid:编号[-编号],多个时以逗号分隔")

	removePrimary = flagBoolean("no-primary-key", "K", false, "对INSERT语句去除主键. 可选.")

	minimalUpdate = flagBoolean("minimal-update", "M", true, "最小化update语句. 可选.")

	minimalInsert = flagBoolean("minimal-insert", "I", true, "使用包含多个VALUES列表的多行语法编写INSERT语句.")

	stopNever = flagBoolean("stop-never", "N", false, "持续解析binlog")

	showGTID    = flagBoolean("show-gtid", "", true, "显示gtid")
	showTime    = flagBoolean("show-time", "", true, "显示执行时间,同一时间仅显示首次")
	showAllTime = flagBoolean("show-all-time", "", false, "显示每条SQL的执行时间")
	showThread  = flagBoolean("show-thread", "", false, "显示线程号,便于区别同一进程操作")

	debug        = flagBoolean("debug", "", false, "调试模式,输出详细日志")
	debugProfile = flagBoolean("debug-profile", "", false, "profile调试")
)

func main() {

	// defer profile.Start(profile.MemProfile).Stop()
	if *debugProfile {
		defer profile.Start(profile.ProfilePath("/tmp")).Stop()
	}

	flag.SortFlags = false

	if err := flag.Parse(os.Args[1:]); err != nil {
		log.Error(err)
		return
	}

	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, "Usage of bingo2sql:\n")
		flag.PrintDefaults()
		return
	}

	// parserProcess = make(map[string]*parser.MyBinlogParser)

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

		ParseDDL:     *parseDDL,
		IncludeGtids: *gtid,

		Databases: *databases,
		Tables:    *tables,
		SqlType:   *sqlType,
		MaxRows:   *maxRows,

		OutputFileStr: *output,

		RemovePrimary: *removePrimary,
		MinimalUpdate: *minimalUpdate,
		MinimalInsert: *minimalInsert,

		ShowGTID:    *showGTID,
		ShowTime:    *showTime,
		ShowAllTime: *showAllTime,
		ShowThread:  *showThread,

		StopNever: *stopNever,
	}

	// thread_id溢出处理
	if *threadID > math.MaxUint32 {
		cfg.ThreadID = uint32(*threadID % (1 << 32))
	} else {
		cfg.ThreadID = uint32(*threadID)
	}

	if p, err := parser.NewBinlogParser(cfg); err != nil {
		log.Error("binlog解析操作失败")
		log.Error(err)
		return
	} else {
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

	viper := viper.New()
	viper.SetConfigFile(*configFile)
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
	group.POST("/parse", parseBinlog)

	// 解析进程的进度
	group.GET("/parse/:id", getParseInfo)

	// 获取所有解析
	group.GET("/parse", getAllParse)

	// 停止解析操作
	group.DELETE("/parse/:id", parseBinlogStop)

	// 下载解析生成的文件
	group.GET("/parse/download/:id", download)

	// router.Logger.Fatal(router.Start(addr))
	router.Logger.Fatal(router.Start(":8077"))
}

func TestMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Do stuff here
		fmt.Println("middleware print: ", r.RequestURI)
		// Call the next handler, which can be another middleware in the chain, or the final handler.
		next.ServeHTTP(w, r)
	})
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "this is home")
}

// getParseInfo 获取解析进程信息
func getParseInfo(c echo.Context) error {
	id := c.Param("id")

	if len(id) == 0 {
		return c.JSON(http.StatusOK, map[string]string{
			"error": "无效参数!",
		})
	}

	if v, ok := parserProcess.Load(id); ok {
		if p, ok := v.(*parser.MyBinlogParser); ok {
			data := &ParseInfo{
				ID:        id,
				ParseRows: p.ParseRows(),
				Percent:   p.Percent(),
			}
			return c.JSON(http.StatusOK, data)
		}
	}

	return c.JSON(http.StatusNotFound, "Not Found!")
}

// getAllParse 获取所有进程信息
func getAllParse(c echo.Context) error {

	// log.Print("当前解析进程数量: ", len(parserProcess.Range()))

	//
	// for _, p := range parserProcess {
	// 	data := &ParseInfo{
	// 		ID:        p.Config().Id(),
	// 		ParseRows: p.ParseRows(),
	// 		Percent:   p.Percent(),
	// 	}
	// 	response = append(response, data)
	// }
	// return c.JSON(http.StatusOK, response)

	count := 0
	// response := make([]*ParseInfo, 0, len(parserProcess))
	response := make([]*ParseInfo, 0)
	// 遍历所有sync.Map中的键值对
	parserProcess.Range(func(k, v interface{}) bool {
		count++
		if p, ok := v.(*parser.MyBinlogParser); ok {
			data := &ParseInfo{
				ID:        p.Config().ID(),
				ParseRows: p.ParseRows(),
				Percent:   p.Percent(),
			}
			response = append(response, data)
		}
		fmt.Println("iterate:", k, v)
		return true
	})
	log.Print("当前解析进程数量: ", count)
	return c.JSON(http.StatusOK, response)

}

func parseBinlog(c echo.Context) error {

	cfg := &parser.BinlogParserConfig{
		ShowGTID: true,
		ShowTime: true,

		MinimalInsert: true,
		MinimalUpdate: true,
	}

	if err := c.Bind(cfg); err != nil {
		return err
	}

	fmt.Println(cfg)
	fmt.Printf("%#v\n", cfg)

	// 指定默认的socket用户,用来生成SQL文件
	if cfg.SocketUser == "" {
		cfg.SocketUser = "test"
	}

	// if cfg.InsID == 0 {
	// 	r := map[string]string{"error": "请指定数据库地址"}
	// 	return c.JSON(http.StatusOK, r)
	// }

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
		id := cfg.ID()
		// parserProcess[id] = p
		parserProcess.Store(id, p)
		go func() {
			// defer delete(parserProcess, id)
			defer parserProcess.Delete(id)
			p.Parser()
		}()

		r := map[string]string{"id": id}
		return c.JSON(http.StatusOK, r)
	}
}

func parseBinlogStop(c echo.Context) error {
	id := c.Param("id")

	if len(id) == 0 {
		return c.JSON(http.StatusOK, map[string]string{
			"error": "无效参数!",
		})
	}

	response := make(map[string]string)

	// log.Print("当前解析进程数量: ", len(parserProcess))

	// defer delete(parserProcess, id)
	defer parserProcess.Delete(id)

	if v, ok := parserProcess.Load(id); ok {
		if p, ok := v.(*parser.MyBinlogParser); ok {
			p.Stop()

			response["ok"] = "1"
			return c.JSON(http.StatusOK, response)
		}
	}

	response["ok"] = "2"
	return c.JSON(http.StatusOK, response)

	// if p, ok := parserProcess[id]; ok {
	// 	p.Stop()

	// 	response["ok"] = "1"
	// 	return c.JSON(http.StatusOK, response)
	// } else {
	// 	response["ok"] = "2"
	// 	return c.JSON(http.StatusOK, response)
	// }
}

func download(c echo.Context) error {
	id := c.Param("id")
	path := "files/" + id + ".tar.gz"

	_, err := os.Stat(path)
	if err != nil {
		log.Error(err)
		return c.JSON(http.StatusNotFound,
			fmt.Sprintf("%s no such file or directory", id))
		// return err
	} else {
		log.Infof("下载路径: %s", path)
		return c.Attachment(path, c.Param("name"))
	}

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
