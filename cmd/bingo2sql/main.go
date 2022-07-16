package main

import (
	"fmt"
	"math"
	"os"
	"sync"

	"github.com/hanchuanchuan/bingo2sql/core"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
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
	tables    = flag.StringP("tables", "t", "", "表名或表结构文件.远程解析时指定表名(可多个,以逗号分隔),本地解析时指定表结构文件")

	threadID = flag.Uint64P("connection-id", "C", 0, "指定线程ID")

	flashback = flagBoolean("flashback", "B", false, "逆向语句")

	parseDDL = flagBoolean("ddl", "", false, "解析DDL语句(仅正向SQL)")

	sqlType = flag.String("sql-type", "insert,delete,update", "解析的语句类型")

	maxRows = flag.Int("max", 100000, "解析的最大行数,设置为0则不限制")
	threads = flag.Int("threads", 64, "解析线程数,影响文件解析速度")

	output = flag.StringP("output", "o", "", "输出到指定文件")

	gtid = flag.StringP("gtid", "g", "", "GTID范围.格式为uuid:编号[-编号],多个时以逗号分隔")

	// removePrimary = flagBoolean("no-primary-key", "K", false, "对INSERT语句去除主键. 可选.")

	minimalUpdate = flagBoolean("minimal-update", "M", true, "最小化update语句. 可选.")

	minimalInsert = flagBoolean("minimal-insert", "I", true, "使用包含多个VALUES列表的多行语法编写INSERT语句.")

	stopNever = flagBoolean("stop-never", "N", false, "持续解析binlog")

	showGTID    = flagBoolean("show-gtid", "", true, "显示gtid")
	showTime    = flagBoolean("show-time", "", true, "显示执行时间,同一时间仅显示首次")
	showAllTime = flagBoolean("show-all-time", "", false, "显示每条SQL的执行时间")
	showThread  = flagBoolean("show-thread", "", false, "显示线程号,便于区别同一进程操作")

	runServer  = flagBoolean("server", "s", false, "启动API服务")
	summary    = flagBoolean("summary", "S", false, "统计binlog文件中的DML次数等信息")
	configFile = flag.StringP("config", "c", "config.ini", "以服务方式启动时需指定配置文件")

	debug = flagBoolean("debug", "", false, "调试模式,输出详细日志")
	mode  = flag.String("profile-mode", "", "enable profiling mode, one of [cpu, mem, mutex, block]")
	// cpuProfile = flagBoolean("cpu-profile", "", false, "调试模式,开启CPU性能跟踪")
)

func main() {
	flag.SortFlags = false
	// 隐藏CPU性能跟踪调试参数
	// flag.MarkHidden("cpu-profile")

	if err := flag.Parse(os.Args[1:]); err != nil {
		log.Error(err)
		return
	}

	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, "Usage of bingo2sql:\n")
		flag.PrintDefaults()
		return
	}

	switch *mode {
	case "cpu":
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	case "mutex":
		defer profile.Start(profile.MutexProfile, profile.ProfilePath(".")).Stop()
	case "block":
		defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()
	default:
		// do nothing
	}

	if *debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.ErrorLevel)
	}
	// 以独立工具运行
	runParse()
}

// runParse 执行binlog解析
func runParse() {

	cfg := &core.BinlogParserConfig{
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
		Threads:   *threads,

		OutputFileStr: *output,

		// RemovePrimary: *removePrimary,
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

	if p, err := core.NewBinlogParser(cfg); err != nil {
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

func flagBoolean(name string, shorthand string, defaultVal bool, usage string) *bool {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
	}
	return flag.BoolP(name, shorthand, defaultVal, usage)
}
