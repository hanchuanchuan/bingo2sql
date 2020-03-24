package bingo2sql

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hanchuanchuan/go-mysql/mysql"
	"github.com/hanchuanchuan/go-mysql/replication"
	"github.com/jinzhu/gorm"
	"github.com/jinzhu/now"
	"github.com/mholt/archiver/v3"
	uuid "github.com/satori/go.uuid"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"

	"github.com/hanchuanchuan/goInception/ast"
	tidb "github.com/hanchuanchuan/goInception/mysql"
	tidbParser "github.com/hanchuanchuan/goInception/parser"
)

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

// Column 列结构
type Column struct {
	gorm.Model
	ColumnName       string `gorm:"Column:COLUMN_NAME"`
	CollationName    string `gorm:"Column:COLLATION_NAME"`
	CharacterSetName string `gorm:"Column:CHARACTER_SET_NAME"`
	ColumnComment    string `gorm:"Column:COLUMN_COMMENT"`
	ColumnType       string `gorm:"Column:COLUMN_TYPE"`
	ColumnKey        string `gorm:"Column:COLUMN_KEY"`
}

// IsUnsigned 是否无符号列
func (f *Column) IsUnsigned() bool {
	columnType := f.ColumnType
	if strings.Contains(columnType, "unsigned") || strings.Contains(columnType, "zerofill") {
		return true
	}
	return false
}

// Table 表结构
type Table struct {
	tableID uint64

	// 仅本地解析使用
	TableName string
	Schema    string

	Columns    []Column
	hasPrimary bool
	primarys   map[int]bool
}

// MasterStatus 主库binlog信息（相对bingo2sql的主库，可以是只读库）
type MasterStatus struct {
	gorm.Model
	File              string `gorm:"Column:File"`
	Position          int    `gorm:"Column:Position"`
	Binlog_Do_DB      string `gorm:"Column:Binlog_Do_DB"`
	Binlog_Ignore_DB  string `gorm:"Column:Binlog_Ignore_DB"`
	Executed_Gtid_Set string `gorm:"Column:Executed_Gtid_Set"`
}

// MasterLog 主库binlog日志
type MasterLog struct {
	gorm.Model
	Name string `gorm:"Column:Log_name"`
	Size int    `gorm:"Column:File_size"`
}

// GtidSetInfo GTID集合,可指定或排除GTID范围
type GtidSetInfo struct {
	uuid       []byte
	startSeqNo int64
	stopSeqNo  int64
}

// BinlogParserConfig 解析选项
type BinlogParserConfig struct {
	Flavor string

	// InsID    int    `json:"ins_id" form:"ins_id"`
	Host     string `json:"host" form:"host"`
	Port     uint16 `json:"port" form:"port"`
	User     string `json:"user" form:"user"`
	Password string `json:"password" form:"password"`

	// binlog文件,位置
	StartFile     string `json:"start_file" form:"start_file"`
	StopFile      string `json:"stop_file" form:"stop_file"`
	StartPosition int    `json:"start_position" form:"start_position"`
	StopPosition  int    `json:"stop_position" form:"stop_position"`

	// 起止时间
	StartTime string `json:"start_time" form:"start_time"`
	StopTime  string `json:"stop_time" form:"stop_time"`

	// 回滚
	Flashback bool `json:"flashback" form:"flashback"`

	// 解析DDL语句
	ParseDDL bool `json:"parse_ddl" form:"parse_ddl"`

	// 限制的数据库
	Databases string `json:"databases" form:"databases"`
	// 限制的表
	Tables string `json:"tables" form:"tables"`
	// sql类型
	SqlType string `json:"sql_type" form:"sql_type"`
	// 最大解析行数
	MaxRows int `json:"max_rows" form:"max_rows"`

	// 输出到指定文件.为空时输出到控制台
	OutputFileStr string

	// 输出所有列(已弃用,有主键时均会使用主键)
	// AllColumns bool `json:"all_columns" form:"all_columns"`

	// debug模式,打印详细日志
	Debug bool `json:"debug" form:"debug"`

	// websocket的用户名,用以发送进度提醒
	SocketUser string `json:"socket_user" form:"socket_user"`

	// 解析任务开始时间
	beginTime int64

	ThreadID uint32 `json:"thread_id" form:"thread_id"`

	IncludeGtids string `json:"include_gtids" form:"include_gtids"`

	// 对INSERT语句去除主键,以便于使用. 默认为false
	RemovePrimary bool
	// 持续解析binlog
	StopNever bool
	// 最小化Update语句, 当开启时,update语句中未变更的值不再记录到解析结果中
	MinimalUpdate bool
	// 使用包含多个VALUES列表的多行语法编写INSERT语句
	MinimalInsert bool

	// 输出设置
	ShowGTID    bool // 输出GTID
	ShowTime    bool // 输出执行时间(相同时间时仅返回首次)
	ShowAllTime bool // 输出所有执行时间
	ShowThread  bool // 输出线程号,方便判断同一执行人的操作
}

type Parser interface {
	write([]byte, *replication.BinlogEvent)
}

// row 协程解析binlog事件的通道
type row struct {
	sql  []byte
	e    *replication.BinlogEvent
	gtid []byte
	opid string

	// 用以打印线程号,不打印时置空
	threadID uint32
}

type baseParser struct {
	// 记录表结构以重用
	allTables map[uint64]*Table

	cfg *BinlogParserConfig

	startFile string
	stopFile  string

	// binlog数据库
	db *gorm.DB

	running bool

	lastTimestamp uint32

	write1 Parser

	ch chan *row

	// currentBackupInfo BackupInfo

	// gtid []byte
	// write interface{}

	OnlyDatabases map[string]bool
	OnlyTables    map[string]bool
	SqlTypes      map[string]bool

	// 本地解析模式.指定binlog文件和表结构本地解析
	localMode bool
}

// MyBinlogParser 解析类
type MyBinlogParser struct {
	baseParser

	masterStatus *MasterStatus

	startTimestamp uint32
	stopTimestamp  uint32
	// 输出到指定文件
	outputFile     *os.File
	outputFileName string

	// db *gorm.DB

	// 使用buffer写的测试
	// 解析600M日志,约500w行,直接使用文件时,用时5min
	// 使用buffer用时1min
	bufferWriter *bufio.Writer

	// running bool

	// lastTimestamp uint32

	gtid      []byte
	lastGtid  []byte
	gtidEvent *replication.GTIDEvent

	includeGtids []*GtidSetInfo

	jumpGtids map[*GtidSetInfo]bool

	// 表结构缓存(仅用于本地解析)
	tableCacheList map[string]*Table

	// 解析用临时变量
	currentPosition  mysql.Position // 当前binlog位置
	currentThreadID  uint32         // 当前event线程号
	changeRows       int            // 解析SQL行数
	currentTimtstamp uint32         // 当前解析到的时间戳
}

var (
	TimeFormat   string = "2006-01-02 15:04:05"
	TimeLocation *time.Location
)

func init() {

	TimeLocation, _ = time.LoadLocation("Asia/Shanghai")
}

// ID 解析任务的唯一标识
func (cfg *BinlogParserConfig) ID() string {

	s1 := strings.Replace(cfg.Host, ".", "_", -1)
	if len(s1) > 20 {
		s1 = s1[:20]
	}
	var s2 string
	if len(cfg.StartTime) > 0 {
		s2 = strings.Replace(cfg.StartTime, ".", "_", -1)
		s2 = strings.Replace(s2, " ", "_", -1)
		s2 = strings.Replace(s2, ":", "", -1)
	} else {
		s2 = strconv.FormatInt(cfg.beginTime, 10)
	}
	return fmt.Sprintf("%s_%d_%s",
		s1,
		cfg.Port,
		s2)
}

// Parser 远程解析
func (p *MyBinlogParser) Parser() error {

	if p.cfg.Host == "" && p.cfg.StartFile != "" {
		_, err := os.Stat(p.cfg.StartFile)
		if err != nil {
			return err
		}
		// if s.IsDir(){

		// }
		return p.parserFile()
	}

	var wg sync.WaitGroup

	defer timeTrack(time.Now(), "Parser")

	defer func() {
		if p.outputFile != nil {
			p.outputFile.Close()
		}
	}()

	var err error

	p.running = true

	p.db, err = p.getDB()
	if err != nil {
		return err
	}

	defer p.db.Close()

	if len(p.outputFileName) > 0 {

		if Exists(p.outputFileName) {
			// p.checkError(errors.New("指定的文件已存在!"))
			// 不作追加,改为文件存在时清除内容,O_APPEND
			p.outputFile, err = os.OpenFile(p.outputFileName,
				os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
		} else {
			p.outputFile, err = os.Create(p.outputFileName)
		}
		if err != nil {
			return err
		}

		p.bufferWriter = bufio.NewWriter(p.outputFile)
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: 2000000111,
		Flavor:   p.cfg.Flavor,

		Host:       p.cfg.Host,
		Port:       p.cfg.Port,
		User:       p.cfg.User,
		Password:   p.cfg.Password,
		UseDecimal: true,
		// RawModeEnabled:  p.cfg.RawMode,
		// SemiSyncEnabled: p.cfg.SemiSync,
	}

	b := replication.NewBinlogSyncer(cfg)
	defer b.Close()

	p.currentPosition = mysql.Position{
		Name: p.startFile,
		Pos:  uint32(p.cfg.StartPosition),
	}

	s, err := b.StartSync(p.currentPosition)
	if err != nil {
		log.Infof("Start sync error: %v\n", err)
		return nil
	}

	sendTime := time.Now().Add(time.Second * 5)
	sendCount := 0

	wg.Add(1)
	go p.ProcessChan(&wg)

	var ctx context.Context
	var cancel context.CancelFunc
	for {
		if p.cfg.StopNever {
			ctx = context.Background()
		} else {
			ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
		}
		// e, err := s.GetEvent(context.Background())
		e, err := s.GetEvent(ctx)
		if err != nil {
			// Try to output all left events
			// events := s.DumpEvents()
			// for _, e := range events {
			//  // e.Dump(os.Stdout)
			//  log.Info("===============")
			//  log.Info(e.Header.EventType)
			// }
			log.Errorf("Get event error: %v\n", err)
			break
		}

		ok, err := p.parseSingleEvent(e)
		if err != nil {
			return err
		}
		if !ok {
			break
		}

		if len(p.cfg.SocketUser) > 0 {
			// 如果指定了websocket用户,就每5s发送一次进度通知
			if p.changeRows > sendCount && time.Now().After(sendTime) {
				sendCount = p.changeRows
				sendTime = time.Now().Add(time.Second * 5)

				kwargs := map[string]interface{}{"rows": p.changeRows}
				if p.stopTimestamp > 0 && p.startTimestamp > 0 && p.stopTimestamp > p.startTimestamp {
					kwargs["pct"] = (e.Header.Timestamp - p.startTimestamp) * 100 / (p.stopTimestamp - p.startTimestamp)
				}
				go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度", "", kwargs)

			}
		}
	}

	close(p.ch)

	wg.Wait()

	if len(p.cfg.SocketUser) > 0 {
		if p.changeRows > 0 {
			kwargs := map[string]interface{}{"rows": p.changeRows}
			kwargs["pct"] = 99

			go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度", "", kwargs)

			// 打包,以便下载
			fileSize, err := p.Archive()
			if err != nil {
				return err
			}

			kwargs = map[string]interface{}{
				"ok":   "1",
				"pct":  100,
				"rows": p.changeRows,
				"size": fileSize}
			go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度",
				"", kwargs)
		} else {

			// 没有数据时清除文件
			p.clear()

			kwargs := map[string]interface{}{"ok": "1", "size": 0, "pct": 100, "rows": 0}

			go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度", "", kwargs)
		}
	}

	log.Info("解析完成")
	return nil
}

// checkFinish 检查是否需要结束循环
func (p *MyBinlogParser) checkFinish(currentPosition *mysql.Position) int {
	returnValue := -1
	// 满足以下任一条件时结束循环
	// * binlog文件超出指定结束文件
	// * 超出指定结束文件的指定位置
	// * 超出当前最新binlog位置

	// 当前binlog解析位置 相对于配置的结束位置/最新位置
	// 超出时返回 1
	// 等于时返回 0
	// 小于时返回 -1

	// 根据是否为事件开始做不同判断
	// 事件开始时做大于判断
	// 事件结束时做等于判断
	var stopMsg string
	if p.stopFile != "" && currentPosition.Name > p.stopFile {
		stopMsg = "超出指定结束文件"
		returnValue = 1
	} else if p.cfg.StopPosition > 0 && currentPosition.Name == p.stopFile {
		if currentPosition.Pos < uint32(p.cfg.StopPosition) {
			stopMsg = "超出指定位置"
			returnValue = 1
		} else if currentPosition.Pos == uint32(p.cfg.StopPosition) {
			stopMsg = "超出指定位置"
			returnValue = 0
		}
	}

	if p.masterStatus != nil {
		if currentPosition.Name > p.masterStatus.File ||
			currentPosition.Name == p.masterStatus.File &&
				currentPosition.Pos > uint32(p.masterStatus.Position) {
			stopMsg = "超出最新binlog位置"
			returnValue = 1
		} else if currentPosition.Name == p.masterStatus.File &&
			currentPosition.Pos == uint32(p.masterStatus.Position) {
			stopMsg = "超出最新binlog位置"
			returnValue = 0
		}
	}

	if stopMsg != "" {
		log.WithFields(log.Fields{
			"当前文件": currentPosition.Name,
			"结束文件": p.stopFile,
			"当前位置": currentPosition.Pos,
			"结束位置": p.cfg.StopPosition,
		}).Info("已超出指定位置")
	}
	return returnValue
}

// clear 压缩完成或者没有数据时删除文件
func (p *MyBinlogParser) clear() {
	err := os.Remove(p.outputFileName)
	if err != nil {
		log.Error(err)
		log.Errorf("删除文件失败! %s", p.outputFileName)
	}
}

func (p *MyBinlogParser) isGtidEventInGtidSet() (status uint8) {

	e := p.gtidEvent

	// 0 正常,包含
	// 1 跳转,不包含
	// 2 超出结束位置,跳过
	if len(p.includeGtids) == 0 {
		status = 0
		return
	}

	if e == nil {
		status = 0
		return
	}

	for _, info := range p.includeGtids {
		if ok, _ := p.jumpGtids[info]; !ok && bytes.Equal(e.SID, info.uuid) {
			if e.GNO < info.startSeqNo {
				status = 1
				return
			} else if e.GNO > info.stopSeqNo {
				p.jumpGtids[info] = true
			} else {
				status = 0
				return
			}
		}
	}

	all_ok := true
	for _, value := range p.jumpGtids {
		if !value {
			all_ok = false
		}
	}

	if all_ok {
		status = 2
		return
	}

	status = 1
	return
}

func (p *MyBinlogParser) Stop() {
	fmt.Println("stoped")

	p.running = false
}

func (p *MyBinlogParser) write(b []byte, binEvent *replication.BinlogEvent) {
	data := &row{
		sql: b,
		e:   binEvent,
	}

	if p.Config().ShowThread {
		data.threadID = p.currentThreadID
	}
	if p.Config().ShowGTID {
		data.gtid = p.gtid
	}
	p.ch <- data
}

// byteEquals 判断字节切片是否相等
func byteEquals(v1, v2 []byte) bool {
	if len(v1) != len(v2) {
		return false
	}
	for i, v := range v1 {
		if v != v2[i] {
			return false
		}
	}
	return true
}

// func (p *MyBinlogParser) myWrite(b []byte, binEvent *replication.BinlogEvent, gtid []byte) {
func (p *MyBinlogParser) myWrite(data *row) {

	var buf bytes.Buffer

	// 输出GTID
	if p.Config().ShowGTID && len(data.gtid) > 0 {
		if len(p.lastGtid) == 0 {
			p.lastGtid = data.gtid
			buf.WriteString("# ")
			buf.Write(data.gtid)
			buf.WriteString("\n")

		} else if !byteEquals(data.gtid, p.lastGtid) {
			p.lastGtid = data.gtid
			buf.WriteString("\n# ")
			buf.Write(data.gtid)
			buf.WriteString("\n")
		}

	}

	buf.Write(data.sql)
	if p.Config().ShowAllTime {
		timeinfo := fmt.Sprintf("; # %s",
			time.Unix(int64(data.e.Header.Timestamp), 0).Format(TimeFormat))
		buf.WriteString(timeinfo)
		if p.Config().ShowThread {
			buf.WriteString(" # thread_id=")
			buf.WriteString(strconv.Itoa(int(data.threadID)))
		}
		buf.WriteString("\n")
	} else if p.Config().ShowTime {
		if p.lastTimestamp != data.e.Header.Timestamp {
			timeinfo := fmt.Sprintf("; # %s",
				time.Unix(int64(data.e.Header.Timestamp), 0).Format(TimeFormat))
			buf.WriteString(timeinfo)
			p.lastTimestamp = data.e.Header.Timestamp
		} else {
			buf.WriteString(";")
		}

		if p.Config().ShowThread {
			buf.WriteString(" # thread_id=")
			buf.WriteString(strconv.Itoa(int(data.threadID)))
		}
		buf.WriteString("\n")

	} else {
		if p.Config().ShowThread {
			buf.WriteString(" # thread_id=")
			buf.WriteString(strconv.Itoa(int(data.threadID)))
		} else {
			buf.WriteString(";")
		}
		buf.WriteString("\n")
	}

	p.write2(buf.Bytes())
}

func (p *MyBinlogParser) write2(b []byte) {

	if len(p.outputFileName) > 0 {
		p.bufferWriter.Write(b)
	} else {
		fmt.Print(string(b))
	}
}

// NewBinlogParser binlog解析器
func NewBinlogParser(cfg *BinlogParserConfig) (*MyBinlogParser, error) {

	p := new(MyBinlogParser)

	p.allTables = make(map[uint64]*Table)
	p.jumpGtids = make(map[*GtidSetInfo]bool)
	p.ch = make(chan *row, 100)

	p.write1 = p

	cfg.beginTime = time.Now().Unix()
	p.cfg = cfg

	p.OnlyDatabases = make(map[string]bool)
	p.OnlyTables = make(map[string]bool)
	p.SqlTypes = make(map[string]bool)

	p.startFile = cfg.StartFile
	p.stopFile = cfg.StopFile

	if err := p.parseGtidSets(); err != nil {
		return nil, err
	}

	if len(cfg.SocketUser) > 0 {
		p.outputFileName = fmt.Sprintf("files/%s.sql", p.cfg.ID())

		// p.outputFileName = fmt.Sprintf("files/%s_%s.sql", time.Now().Format("20060102_1504"), p.cfg.Id())

		// var fileName []string
		// fileName = append(fileName, strings.Replace(cfg.Host, ".", "_", -1))
		// fileName = append(fileName, strconv.Itoa(int(cfg.Port)))
		// if cfg.Databases != "" {
		// 	fileName = append(fileName, cfg.Databases)
		// }
		// if cfg.Tables != "" {
		// 	fileName = append(fileName, cfg.Tables)
		// }

		// fileName = append(fileName, time.Now().Format("20060102_1504"))

		// if cfg.Flashback {
		// 	fileName = append(fileName, "rollback")
		// }

		// p.outputFileName = fmt.Sprintf("files/%s.sql", strings.Join(fileName, "_"))
	} else {
		if cfg.OutputFileStr != "" {
			p.outputFileName = cfg.OutputFileStr
		} else {
			p.outputFile = os.Stdout
		}
	}

	// 本地解析模式,host为空,表名为SQL文件
	if p.cfg.Host == "" {
		if p.cfg.Tables == "" {
			return nil, fmt.Errorf("本地解析模式请指定表结构文件--tables")
		}
		_, err := os.Stat(p.cfg.Tables)
		if err != nil {
			return nil, fmt.Errorf("读取表结构文件失败(%s): %v", p.cfg.Tables, err)
		}

		if err := p.readTableSchema(p.cfg.Tables); err != nil {
			return nil, fmt.Errorf("读取表结构文件失败(%s): %v", p.cfg.Tables, err)
		}

		if len(p.tableCacheList) == 0 {
			return nil, fmt.Errorf("未找到建表语句! 请提供需要解析的表对应的建表语句,并以分号分隔.")
		} else {
			log.Infof("共读取到表结构 %d 个", len(p.tableCacheList))
		}

		p.localMode = true
	}

	if !p.localMode {
		var err error
		p.db, err = p.getDB()
		if err != nil {
			return nil, err
		}
		defer p.db.Close()

		p.masterStatus, err = p.mysqlMasterStatus()
		if err != nil {
			return nil, err
		}

		if p.cfg.Debug {
			p.db.LogMode(true)
		}
	}

	if err := p.parserInit(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *MyBinlogParser) ProcessChan(wg *sync.WaitGroup) {
	for {
		r := <-p.ch
		if r == nil {
			if len(p.outputFileName) > 0 {
				p.bufferWriter.Flush()
			}
			wg.Done()
			break
		}
		// p.myWrite(r.sql, r.e, r.gtid)
		p.myWrite(r)
	}
}

// parseGtidSets 解析gtid集合
func (p *MyBinlogParser) parseGtidSets() error {
	if len(p.cfg.IncludeGtids) == 0 {
		return nil
	}

	sets := strings.Split(p.cfg.IncludeGtids, ",")

	for _, s := range sets {
		g := &GtidSetInfo{}
		str := strings.Split(s, ":")
		if len(str) != 2 {
			return errors.New("错误GTID格式!正确格式为uuid:编号[-编号],多个时以逗号分隔")
		}

		// 把uuid逆向为16位[]byte
		u2, err := uuid.FromString(str[0])
		if err != nil {
			return fmt.Errorf("GTID解析失败!(%s)", err.Error())
		}

		g.uuid = u2.Bytes()

		nos := strings.Split(str[1], "-")
		if len(nos) == 1 {
			n, err := strconv.ParseInt(nos[0], 10, 64)
			if err != nil {
				return fmt.Errorf("GTID解析失败!(%s)", err.Error())
			}
			g.startSeqNo = n
			g.stopSeqNo = n
		} else {
			n, err := strconv.ParseInt(nos[0], 10, 64)
			if err != nil {
				return fmt.Errorf("GTID解析失败!(%s)", err.Error())
			}
			n2, err := strconv.ParseInt(nos[1], 10, 64)
			if err != nil {
				return fmt.Errorf("GTID解析失败!(%s)", err.Error())
			}
			g.startSeqNo = n
			g.stopSeqNo = n2
		}

		p.jumpGtids[g] = false
		p.includeGtids = append(p.includeGtids, g)
		// fmt.Println(*g)
	}

	log.Debugf("gtid集合数量: %d", len(p.includeGtids))

	return nil
}

func (p *baseParser) getDB() (*gorm.DB, error) {
	if p.cfg.Host == "" {
		return nil, fmt.Errorf("请指定数据库地址")
	}
	if p.cfg.Port == 0 {
		return nil, fmt.Errorf("请指定数据库端口")
	}
	if p.cfg.User == "" {
		return nil, fmt.Errorf("请指定数据库用户名")
	}
	addr := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql?charset=utf8mb4&parseTime=True&loc=Local",
		p.cfg.User, p.cfg.Password, p.cfg.Host, p.cfg.Port)

	return gorm.Open("mysql", addr)
}

// parserInit 解析服务初始化
func (p *MyBinlogParser) parserInit() error {

	defer timeTrack(time.Now(), "parserInit")

	if len(p.outputFileName) > 0 {
		var err error
		if Exists(p.outputFileName) {
			// p.checkError(errors.New("指定的文件已存在!"))
			// 不作追加,改为文件存在时清除内容,O_APPEND
			p.outputFile, err = os.OpenFile(p.outputFileName,
				os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
		} else {
			p.outputFile, err = os.Create(p.outputFileName)
		}
		p.checkError(err)

		// outputFile, err := os.OpenFile(outputFileStr, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
		// if err != nil {
		//  log.Info(index, "open file failed.", err.Error())
		//  break
		// }
		defer p.outputFile.Close()

		p.bufferWriter = bufio.NewWriter(p.outputFile)
	}

	if p.cfg.StartTime != "" {
		t, err := now.Parse(p.cfg.StartTime)
		if err != nil {
			return err
		}
		p.startTimestamp = uint32(t.Unix())
	}
	if p.cfg.StopTime != "" {
		t, err := now.Parse(p.cfg.StopTime)
		if err != nil {
			return err
		}
		p.stopTimestamp = uint32(t.Unix())
	}

	// 如果未指定开始文件,就自动解析
	if len(p.startFile) == 0 {

		binlogIndex := p.autoParseBinlogPosition()
		if len(binlogIndex) == 0 {
			p.checkError(errors.New("无法获取master binlog"))
		}

		p.startFile = binlogIndex[0].Name

		if p.startTimestamp > 0 || p.stopTimestamp > 0 {
			for _, masterLog := range binlogIndex {
				timestamp, err := p.getBinlogFirstTimestamp(masterLog.Name)
				p.checkError(err)

				log.WithFields(log.Fields{
					"起始时间":       time.Unix(int64(timestamp), 0).Format(TimeFormat),
					"binlogFile": masterLog.Name,
				}).Info("binlog信息")

				if timestamp <= p.startTimestamp {
					p.startFile = masterLog.Name
				}

				if p.stopTimestamp > 0 && timestamp <= p.stopTimestamp {
					p.stopFile = masterLog.Name
					break
				}
			}
		}

		if len(p.startFile) == 0 {
			p.checkError(errors.New("未能解析指定时间段的binlog文件,请检查后重试!"))
		}

		log.Infof("根据指定的时间段,解析出的开始binlog文件是:%s,结束文件是:%s\n",
			p.startFile, p.stopFile)
	}

	// // 未指定结束文件时,仅解析该文件
	// if len(p.stopFile) == 0 {
	// 	p.stopFile = p.startFile
	// }

	if len(p.cfg.SqlType) > 0 {
		for _, s := range strings.Split(p.cfg.SqlType, ",") {
			p.SqlTypes[s] = true
		}
	} else {
		p.SqlTypes["insert"] = true
		p.SqlTypes["update"] = true
		p.SqlTypes["delete"] = true
	}

	if len(p.cfg.Databases) > 0 {
		for _, s := range strings.Split(p.cfg.Databases, ",") {
			p.OnlyDatabases[s] = true
		}
	}

	if len(p.cfg.Tables) > 0 {
		for _, s := range strings.Split(p.cfg.Tables, ",") {
			p.OnlyTables[s] = true
		}
	}

	return nil
}

func Exists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil || os.IsExist(err)
}

// getBinlogFirstTimestamp 获取binlog文件第一个时间戳
func (p *MyBinlogParser) getBinlogFirstTimestamp(file string) (uint32, error) {

	logLevel := log.GetLevel()
	defer func() {
		log.SetLevel(logLevel)
	}()
	// 临时调整日志级别,忽略close sync异常
	log.SetLevel(log.FatalLevel)

	cfg := replication.BinlogSyncerConfig{
		ServerID: 2000000110,
		Flavor:   p.cfg.Flavor,

		Host:     p.cfg.Host,
		Port:     p.cfg.Port,
		User:     p.cfg.User,
		Password: p.cfg.Password,
		// RawModeEnabled:  p.cfg.RawMode,
		// SemiSyncEnabled: p.cfg.SemiSync,
	}

	b := replication.NewBinlogSyncer(cfg)

	pos := mysql.Position{Name: file,
		Pos: uint32(4)}

	s, err := b.StartSync(pos)
	if err != nil {
		return 0, err
	}

	defer func() {
		b.Close()
	}()
	for {
		// return
		e, err := s.GetEvent(context.Background())
		if err != nil {
			// b.Close()
			return 0, err
		}

		// log.Infof("事件类型:%s", e.Header.EventType)
		if e.Header.Timestamp > 0 {
			// b.Close()
			return e.Header.Timestamp, nil
		}
	}
}

// check 读取文件的函数调用大多数都需要检查错误，
// 使用下面这个错误检查方法可以方便一点
func check(e error) {
	if e != nil {
		panic(e)
	}
}

func timeParseToUnix(timeStr string) (uint32, error) {
	// t, err := time.Parse(TimeFormat, *timeStr)
	t, err := time.ParseInLocation(TimeFormat, timeStr, TimeLocation)

	if err != nil {
		return 0, err
	}
	stamp := t.Unix()
	return uint32(stamp), nil
}

func (p *MyBinlogParser) checkError(e error) {
	if e != nil {
		log.Error(e)

		if len(p.cfg.SocketUser) > 0 {
			kwargs := map[string]interface{}{"error": e.Error()}
			sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度",
				"", kwargs)
		}
		panic(e)
	}
}

func (p *MyBinlogParser) schemaFilter(table *replication.TableMapEvent) bool {
	if len(p.OnlyDatabases) == 0 && len(p.OnlyTables) == 0 {
		return true
	}

	if len(p.OnlyDatabases) > 0 {
		if _, ok := (p.OnlyDatabases)[string(table.Schema)]; !ok {
			return false
		}
	}
	if len(p.OnlyTables) > 0 {
		if _, ok := (p.OnlyTables)[strings.ToLower(string(table.Table))]; !ok {
			return false
		}
	}
	return true
}

// generateInsertSQL 生成insert语句
func (p *MyBinlogParser) generateInsertSQL(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {
	tableName := getTableName(e)
	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s缺少列!当前列数:%d,binlog的列数%d",
			tableName, len(t.Columns), e.ColumnCount)
	}

	var columnNames []string
	c := "`%s`"
	template := "INSERT INTO %s(%s) VALUES(%s)"
	if p.cfg.MinimalInsert && len(e.Rows) > 1 {
		template = "INSERT INTO %s(%s) VALUES%s"
	}

	for i, col := range t.Columns {
		if i < int(e.ColumnCount) {
			//  有主键且设置去除主键时 作特殊处理
			if t.hasPrimary && p.cfg.RemovePrimary {
				if _, ok := t.primarys[i]; !ok {
					columnNames = append(columnNames, fmt.Sprintf(c, col.ColumnName))
				}
			} else {
				columnNames = append(columnNames, fmt.Sprintf(c, col.ColumnName))
			}
		}
	}

	paramValues := strings.Repeat("?,", len(columnNames))
	paramValues = strings.TrimRight(paramValues, ",")

	if p.cfg.MinimalInsert && len(e.Rows) > 1 {
		paramValues = strings.Repeat("("+paramValues+"),", len(e.Rows))
		paramValues = strings.TrimRight(paramValues, ",")
	}

	sql := fmt.Sprintf(template, tableName, strings.Join(columnNames, ","), paramValues)

	var vv []driver.Value
	for _, rows := range e.Rows {

		for i, d := range rows {
			if t.hasPrimary && p.cfg.RemovePrimary {
				if _, ok := t.primarys[i]; ok {
					continue
				}
			}

			if t.Columns[i].IsUnsigned() {
				d = processValue(d, GetDataTypeBase(t.Columns[i].ColumnType))
			}
			vv = append(vv, d)
		}

		if !p.cfg.MinimalInsert || len(e.Rows) == 1 {
			r, err := InterpolateParams(sql, vv)
			if err != nil {
				log.Error(err)
			}
			p.write(r, binEvent)
			vv = nil
		}
	}

	if p.cfg.MinimalInsert && len(e.Rows) > 1 {
		r, err := InterpolateParams(sql, vv)
		if err != nil {
			log.Error(err)
		}
		p.write(r, binEvent)
	}

	return nil
}

// generateDeleteSQL 生成delete语句
func (p *MyBinlogParser) generateDeleteSQL(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {
	tableName := getTableName(e)
	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s缺少列!当前列数:%d,binlog的列数%d",
			tableName, len(t.Columns), e.ColumnCount)
	}

	template := "DELETE FROM %s WHERE"

	sql := fmt.Sprintf(template, tableName)

	c_null := " `%s` IS ?"
	c := " `%s`=?"
	var columnNames []string
	for _, rows := range e.Rows {

		columnNames = nil

		var vv []driver.Value
		for i, d := range rows {
			if t.hasPrimary {
				_, ok := t.primarys[i]
				if ok {
					if t.Columns[i].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[i].ColumnType))
					}
					vv = append(vv, d)
					if d == nil {
						columnNames = append(columnNames,
							fmt.Sprintf(c_null, t.Columns[i].ColumnName))
					} else {
						columnNames = append(columnNames,
							fmt.Sprintf(c, t.Columns[i].ColumnName))
					}
				}
			} else {
				if t.Columns[i].IsUnsigned() {
					d = processValue(d, GetDataTypeBase(t.Columns[i].ColumnType))
				}
				vv = append(vv, d)

				if d == nil {
					columnNames = append(columnNames,
						fmt.Sprintf(c_null, t.Columns[i].ColumnName))
				} else {
					columnNames = append(columnNames,
						fmt.Sprintf(c, t.Columns[i].ColumnName))
				}
			}
		}
		newSql := strings.Join([]string{sql, strings.Join(columnNames, " AND")}, "")

		r, err := InterpolateParams(newSql, vv)
		if err != nil {
			log.Error(err)
		}

		p.write(r, binEvent)

	}

	return nil
}

// processValue 处理无符号值(unsigned)
func processValue(value driver.Value, dataType string) driver.Value {
	if value == nil {
		return value
	}

	switch v := value.(type) {
	case int8:
		if v >= 0 {
			return value
		}
		return int64(1<<8 + int64(v))
	case int16:
		if v >= 0 {
			return value
		}
		return int64(1<<16 + int64(v))
	case int32:
		if v >= 0 {
			return value
		}
		if dataType == "mediumint" {
			return int64(1<<24 + int64(v))
		}
		return int64(1<<32 + int64(v))
	case int64:
		if v >= 0 {
			return value
		}
		return math.MaxUint64 - uint64(abs(v)) + 1
	// case int:
	// case float32:
	// case float64:

	default:
		// log.Error("解析错误")
		// log.Errorf("%T", v)
		return value
	}
}

func abs(n int64) int64 {
	y := n >> 63
	return (n ^ y) - y
}

// generateUpdateSQL 生成update语句
func (p *MyBinlogParser) generateUpdateSQL(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {
	tableName := getTableName(e)
	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s缺少列!当前列数:%d,binlog的列数%d",
			tableName, len(t.Columns), e.ColumnCount)
	}

	template := "UPDATE %s SET%s WHERE"

	setValue := " `%s`=?"

	var columnNames []string
	c_null := " `%s` IS ?"
	c := " `%s`=?"

	var sql string
	var sets []string

	// for i, col := range t.Columns {
	// 	if i < int(e.ColumnCount) {
	// 		sets = append(sets, fmt.Sprintf(setValue, col.ColumnName))
	// 	}
	// }

	// 最小化回滚语句, 当开启时,update语句中未变更的值不再记录到回滚语句中
	minimalMode := p.cfg.MinimalUpdate

	if !minimalMode {
		for i, col := range t.Columns {
			if i < int(e.ColumnCount) {
				sets = append(sets, fmt.Sprintf(setValue, col.ColumnName))
			}
		}
		sql = fmt.Sprintf(template, tableName, strings.Join(sets, ","))
	}

	// sql := fmt.Sprintf(template,tableName,
	// 	strings.Join(sets, ","))

	var (
		oldValues []driver.Value
		newValues []driver.Value
		newSql    string
	)
	// update时, Rows为2的倍数, 双数index为旧值,单数index为新值
	for i, rows := range e.Rows {

		if i%2 == 0 {
			// 旧值
			columnNames = nil

			for j, d := range rows {
				if t.hasPrimary {
					_, ok := t.primarys[j]
					if ok {
						oldValues = append(oldValues, d)

						if d == nil {
							columnNames = append(columnNames,
								fmt.Sprintf(c_null, t.Columns[j].ColumnName))
						} else {
							columnNames = append(columnNames,
								fmt.Sprintf(c, t.Columns[j].ColumnName))
						}
					}
				} else {
					if t.Columns[j].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[j].ColumnType))
					}
					oldValues = append(oldValues, d)

					if d == nil {
						columnNames = append(columnNames,
							fmt.Sprintf(c_null, t.Columns[j].ColumnName))
					} else {
						columnNames = append(columnNames,
							fmt.Sprintf(c, t.Columns[j].ColumnName))
					}
				}
			}
		} else {
			// 新值
			for j, d := range rows {
				if minimalMode {
					// 最小化模式下,列如果相等则省略
					if !compareValue(d, e.Rows[i-1][j]) {
						if t.Columns[j].IsUnsigned() {
							d = processValue(d, GetDataTypeBase(t.Columns[j].ColumnType))
						}
						newValues = append(newValues, d)
						if j < len(t.Columns) {
							sets = append(sets, fmt.Sprintf(setValue, t.Columns[j].ColumnName))
						}
					}
				} else {
					if t.Columns[j].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[j].ColumnType))
					}
					newValues = append(newValues, d)
				}
			}

			newValues = append(newValues, oldValues...)
			if minimalMode {
				sql = fmt.Sprintf(template, tableName,
					strings.Join(sets, ","))
				sets = nil
			}
			newSql = strings.Join([]string{sql, strings.Join(columnNames, " AND")}, "")
			// log.Info(newSql, len(newValues))
			r, err := InterpolateParams(newSql, newValues)
			p.checkError(err)

			p.write(r, binEvent)

			oldValues = nil
			newValues = nil
		}

	}

	return nil
}

// generateUpdateRollbackSQL 生成update语句
func (p *MyBinlogParser) generateUpdateRollbackSQL(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {
	tableName := getTableName(e)
	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s缺少列!当前列数:%d,binlog的列数%d",
			tableName, len(t.Columns), e.ColumnCount)
	}

	template := "UPDATE %s SET%s WHERE"

	setValue := " `%s`=?"

	var columnNames []string
	c_null := " `%s` IS ?"
	c := " `%s`=?"

	var sql string
	var sets []string

	// 最小化回滚语句, 当开启时,update语句中未变更的值不再记录到回滚语句中
	minimalMode := p.cfg.MinimalUpdate

	if !minimalMode {
		for i, col := range t.Columns {
			if i < int(e.ColumnCount) {
				sets = append(sets, fmt.Sprintf(setValue, col.ColumnName))
			}
		}
		sql = fmt.Sprintf(template, tableName, strings.Join(sets, ","))
	}

	var (
		oldValues []driver.Value
		newValues []driver.Value
		newSql    string
	)
	// update时, Rows为2的倍数, 双数index为旧值,单数index为新值
	for i, rows := range e.Rows {

		if i%2 == 0 {
			// 旧值
			for j, d := range rows {
				if minimalMode {
					// 最小化模式下,列如果相等则省略
					if !compareValue(d, e.Rows[i+1][j]) {
						if t.Columns[j].IsUnsigned() {
							d = processValue(d, GetDataTypeBase(t.Columns[j].ColumnType))
						}
						newValues = append(newValues, d)
						if j < len(t.Columns) {
							sets = append(sets, fmt.Sprintf(setValue, t.Columns[j].ColumnName))
						}
					}
				} else {
					if t.Columns[j].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[j].ColumnType))
					}
					newValues = append(newValues, d)
				}
			}
		} else {
			// 新值
			if p.cfg.Flashback {
				columnNames = nil
			}
			for j, d := range rows {
				if t.hasPrimary {
					_, ok := t.primarys[j]
					if ok {
						if t.Columns[j].IsUnsigned() {
							d = processValue(d, GetDataTypeBase(t.Columns[j].ColumnType))
						}
						oldValues = append(oldValues, d)

						if d == nil {
							columnNames = append(columnNames,
								fmt.Sprintf(c_null, t.Columns[j].ColumnName))
						} else {
							columnNames = append(columnNames,
								fmt.Sprintf(c, t.Columns[j].ColumnName))
						}
					}
				} else {
					if t.Columns[j].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[j].ColumnType))
					}
					oldValues = append(oldValues, d)

					if d == nil {
						columnNames = append(columnNames,
							fmt.Sprintf(c_null, t.Columns[j].ColumnName))
					} else {
						columnNames = append(columnNames,
							fmt.Sprintf(c, t.Columns[j].ColumnName))
					}
				}
			}

			newValues = append(newValues, oldValues...)
			if minimalMode {
				sql = fmt.Sprintf(template, tableName, strings.Join(sets, ","))
				sets = nil
			}
			newSql = strings.Join([]string{sql, strings.Join(columnNames, " AND")}, "")
			r, err := InterpolateParams(newSql, newValues)
			p.checkError(err)

			p.write(r, binEvent)

			oldValues = nil
			newValues = nil
		}
	}

	return nil
}
func (p *MyBinlogParser) tableInformation(tableId uint64, schema []byte, tableName []byte) (*Table, error) {

	table, ok := p.allTables[tableId]
	if ok {
		return table, nil
	}

	if p.localMode && len(p.tableCacheList) > 0 {
		key := strings.ToLower(fmt.Sprintf("%s.%s", string(schema), string(tableName)))
		if table, ok := p.tableCacheList[key]; ok {
			p.allTables[tableId] = table
			return table, nil
		} else {
			key = strings.ToLower(string(tableName))
			if table, ok := p.tableCacheList[key]; ok {
				p.allTables[tableId] = table
				return table, nil
			}
		}
		return nil, nil
	}

	sql := `SELECT COLUMN_NAME, ifnull(COLLATION_NAME,'') as COLLATION_NAME,
            ifnull(CHARACTER_SET_NAME,'') as CHARACTER_SET_NAME,
            ifnull(COLUMN_COMMENT,'') as COLUMN_COMMENT, COLUMN_TYPE,
            ifnull(COLUMN_KEY,'') as COLUMN_KEY
            FROM information_schema.columns
            WHERE table_schema = ? and table_name = ?
            ORDER BY ORDINAL_POSITION`

	var allRecord []Column

	p.db.Raw(sql, string(schema), string(tableName)).Scan(&allRecord)

	var primarys map[int]bool
	primarys = make(map[int]bool)

	var uniques map[int]bool
	uniques = make(map[int]bool)
	for i, r := range allRecord {
		if r.ColumnKey == "PRI" {
			primarys[i] = true
		}
		if r.ColumnKey == "UNI" {
			uniques[i] = true
		}
	}

	newTable := new(Table)
	newTable.tableID = tableId
	newTable.Columns = allRecord

	// if !p.cfg.AllColumns {
	if len(primarys) > 0 {
		newTable.primarys = primarys
		newTable.hasPrimary = true
	} else if len(uniques) > 0 {
		newTable.primarys = uniques
		newTable.hasPrimary = true
	} else {
		newTable.hasPrimary = false
	}
	// }

	p.allTables[tableId] = newTable

	return newTable, nil
}

func (p *MyBinlogParser) mysqlMasterStatus() (*MasterStatus, error) {

	defer timeTrack(time.Now(), "mysqlMasterStatus")

	// rows, err := db.Query("SHOW MASTER STATUS")
	// p.checkError(err)

	// defer rows.Close()

	r := MasterStatus{}

	p.db.Raw("SHOW MASTER STATUS").Scan(&r)

	// columns, _ := rows.Columns()

	// r := MasterStatus{}
	// for rows.Next() {
	//  if len(columns) == 4 {
	//      err = rows.Scan(&(r.File), &(r.Position), &(r.Binlog_Do_DB),
	//          &(r.Binlog_Ignore_DB))
	//  } else if len(columns) == 5 {
	//      err = rows.Scan(&(r.File), &(r.Position), &(r.Binlog_Do_DB),
	//          &(r.Binlog_Ignore_DB), &(r.Executed_Gtid_Set))
	//  } else {
	//      return nil, errors.New("show master status返回的列数无法解析!!!")
	//  }
	//  p.checkError(err)
	// }

	return &r, nil
}

func (p *MyBinlogParser) autoParseBinlogPosition() []MasterLog {

	var binlogIndex []MasterLog
	p.db.Raw("SHOW MASTER LOGS").Scan(&binlogIndex)

	return binlogIndex
}

// InterpolateParams 填充占位符参数
func InterpolateParams(query string, args []driver.Value) ([]byte, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		log.WithFields(log.Fields{
			"需要参数": strings.Count(query, "?"),
			"提供参数": len(args),
		}).Error("sql的参数个数不匹配")

		return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
	}

	var buf []byte

	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf = append(buf, query[i:]...)
			break
		}
		buf = append(buf, query[i:i+q]...)
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf = append(buf, "NULL"...)
			continue
		}

		switch v := arg.(type) {
		case int8:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int16:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int32:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case int64:
			buf = strconv.AppendInt(buf, v, 10)
		case uint64:
			buf = strconv.AppendUint(buf, uint64(v), 10)
		case int:
			buf = strconv.AppendInt(buf, int64(v), 10)
		case decimal.Decimal:
			buf = append(buf, v.String()...)
		case float32:
			buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
		case float64:
			buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
		case bool:
			if v {
				buf = append(buf, '1')
			} else {
				buf = append(buf, '0')
			}
		case time.Time:
			if v.IsZero() {
				buf = append(buf, "'0000-00-00'"...)
			} else {
				v := v.In(time.UTC)
				v = v.Add(time.Nanosecond * 500) // To round under microsecond
				year := v.Year()
				year100 := year / 100
				year1 := year % 100
				month := v.Month()
				day := v.Day()
				hour := v.Hour()
				minute := v.Minute()
				second := v.Second()
				micro := v.Nanosecond() / 1000

				buf = append(buf, []byte{
					'\'',
					digits10[year100], digits01[year100],
					digits10[year1], digits01[year1],
					'-',
					digits10[month], digits01[month],
					'-',
					digits10[day], digits01[day],
					' ',
					digits10[hour], digits01[hour],
					':',
					digits10[minute], digits01[minute],
					':',
					digits10[second], digits01[second],
				}...)

				if micro != 0 {
					micro10000 := micro / 10000
					micro100 := micro / 100 % 100
					micro1 := micro % 100
					buf = append(buf, []byte{
						'.',
						digits10[micro10000], digits01[micro10000],
						digits10[micro100], digits01[micro100],
						digits10[micro1], digits01[micro1],
					}...)
				}
				buf = append(buf, '\'')
			}
		case string:
			buf = append(buf, '\'')
			buf = escapeBytesBackslash(buf, []byte(v))
			buf = append(buf, '\'')
		case []byte:
			if v == nil {
				buf = append(buf, "NULL"...)
			} else {
				// buf = append(buf, "_binary'"...)
				buf = append(buf, '\'')

				buf = escapeBytesBackslash(buf, v)

				buf = append(buf, '\'')
			}
		default:
			// fmt.Println(v)
			log.Printf("%T", v)
			log.Info("解析错误")
			return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
		}

		// 4 << 20 , 4MB
		// 移除对单行大小的判断,不入库不需要该限制
		// if len(buf)+4 > 4<<20 {
		// 	// log.Print("%T", v)
		// 	log.Info("解析错误")
		// 	return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
		// }
	}
	if argPos != len(args) {
		// log.Print("%T", v)
		log.Info("解析错误")
		return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
	}
	return buf, nil
}

// reserveBuffer checks cap(buf) and expand buffer to len(buf) + appendSize.
// If cap(buf) is not enough, reallocate new buffer.
func reserveBuffer(buf []byte, appendSize int) []byte {
	newSize := len(buf) + appendSize
	if cap(buf) < newSize {
		// Grow buffer exponentially
		newBuf := make([]byte, len(buf)*2+appendSize)
		copy(newBuf, buf)
		buf = newBuf
	}
	return buf[:newSize]
}

// escapeBytesBackslash escapes []byte with backslashes (\)
// This escapes the contents of a string (provided as []byte) by adding backslashes before special
// characters, and turning others into specific escape sequences, such as
// turning newlines into \n and null bytes into \0.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L823-L932
func escapeBytesBackslash(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringBackslash is similar to escapeBytesBackslash but for string.
func escapeStringBackslash(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		switch c {
		case '\x00':
			buf[pos] = '\\'
			buf[pos+1] = '0'
			pos += 2
		case '\n':
			buf[pos] = '\\'
			buf[pos+1] = 'n'
			pos += 2
		case '\r':
			buf[pos] = '\\'
			buf[pos+1] = 'r'
			pos += 2
		case '\x1a':
			buf[pos] = '\\'
			buf[pos+1] = 'Z'
			pos += 2
		case '\'':
			buf[pos] = '\\'
			buf[pos+1] = '\''
			pos += 2
		case '"':
			buf[pos] = '\\'
			buf[pos+1] = '"'
			pos += 2
		case '\\':
			buf[pos] = '\\'
			buf[pos+1] = '\\'
			pos += 2
		default:
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeBytesQuotes escapes apostrophes in []byte by doubling them up.
// This escapes the contents of a string by doubling up any apostrophes that
// it contains. This is used when the NO_BACKSLASH_ESCAPES SQL_MODE is in
// effect on the server.
// https://github.com/mysql/mysql-server/blob/mysql-5.7.5/mysys/charset.c#L963-L1038
func escapeBytesQuotes(buf, v []byte) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for _, c := range v {
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// escapeStringQuotes is similar to escapeBytesQuotes but for string.
func escapeStringQuotes(buf []byte, v string) []byte {
	pos := len(buf)
	buf = reserveBuffer(buf, len(v)*2)

	for i := 0; i < len(v); i++ {
		c := v[i]
		if c == '\'' {
			buf[pos] = '\''
			buf[pos+1] = '\''
			pos += 2
		} else {
			buf[pos] = c
			pos++
		}
	}

	return buf[:pos]
}

// GetDataTypeBase 获取dataType中的数据类型，忽略长度
func GetDataTypeBase(dataType string) string {
	if i := strings.Index(dataType, "("); i > 0 {
		return dataType[0:i]
	}

	return dataType
}

// readTableSchema 读取表结构
func (p *MyBinlogParser) readTableSchema(path string) error {
	fileObj, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fileObj.Close()

	reader := bufio.NewReader(fileObj)
	p.tableCacheList = make(map[string]*Table)

	var buf []string
	quotaIsDouble := true
	parser := tidbParser.New()
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Println(err)
				break
			}
		}

		if strings.Count(line, "'")%2 == 1 {
			quotaIsDouble = !quotaIsDouble
		}

		if ((strings.HasSuffix(line, ";") || strings.HasSuffix(line, ";\r")) &&
			quotaIsDouble) || err == io.EOF {
			buf = append(buf, line)
			s1 := strings.Join(buf, "\n")
			s1 = strings.TrimRight(s1, ";")
			buf = nil
			stmtNodes, _, err := parser.Parse(s1, "utf8mb4", "utf8mb4_bin")
			if err != nil {
				return fmt.Errorf("解析失败: %v", err)
			}

			for _, stmtNode := range stmtNodes {
				//  是ASCII码160的特殊空格
				// currentSql := strings.Trim(stmtNode.Text(), " ;\t\n\v\f\r ")
				switch node := stmtNode.(type) {
				case *ast.CreateTableStmt:
					p.cacheNewTable(buildTableInfo(node))
				}
			}
		} else {
			buf = append(buf, line)
		}

		if err == io.EOF {
			break
		}
	}
	return nil
}

// cacheNewTable 缓存表结构
func (p *MyBinlogParser) cacheNewTable(t *Table) {

	var key string
	if t.Schema == "" {
		key = t.TableName
	} else {
		key = fmt.Sprintf("%s.%s", t.Schema, t.TableName)
	}

	key = strings.ToLower(key)

	p.OnlyTables[strings.ToLower(t.TableName)] = true
	p.tableCacheList[key] = t
}

// buildTableInfo 构建表结构
func buildTableInfo(node *ast.CreateTableStmt) *Table {
	table := &Table{
		Schema:    node.Table.Schema.String(),
		TableName: node.Table.Name.String(),
	}

	for _, ct := range node.Constraints {
		switch ct.Tp {
		// 设置主键标志
		case ast.ConstraintPrimaryKey:
			for _, col := range ct.Keys {
				for _, field := range node.Cols {
					if field.Name.Name.L == col.Column.Name.L {
						field.Tp.Flag |= tidb.PriKeyFlag
						break
					}
				}
			}
		case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
			for _, col := range ct.Keys {
				for _, field := range node.Cols {
					if field.Name.Name.L == col.Column.Name.L {
						field.Tp.Flag |= tidb.UniqueKeyFlag
						break
					}
				}
			}
		}
	}

	table.Columns = make([]Column, 0, len(node.Cols))
	for _, field := range node.Cols {
		table.Columns = append(table.Columns, *buildNewColumnToCache(table, field))
	}

	table.configPrimaryKey()

	return table
}

// buildNewColumnToCache 构建列
func buildNewColumnToCache(t *Table, field *ast.ColumnDef) *Column {
	c := &Column{}

	c.ColumnName = field.Name.Name.String()
	c.ColumnType = field.Tp.InfoSchemaStr()

	for _, op := range field.Options {
		switch op.Tp {
		case ast.ColumnOptionComment:
			c.ColumnComment = op.Expr.GetDatum().GetString()

		case ast.ColumnOptionPrimaryKey:
			c.ColumnKey = "PRI"
		case ast.ColumnOptionUniqKey:
			c.ColumnKey = "UNI"
		case ast.ColumnOptionAutoIncrement:
			// c.Extra += "auto_increment"
		}
	}

	if c.ColumnKey != "PRI" && tidb.HasPriKeyFlag(field.Tp.Flag) {
		c.ColumnKey = "PRI"
	} else if tidb.HasUniKeyFlag(field.Tp.Flag) {
		c.ColumnKey = "UNI"
	}
	return c
}

// compareValue 比较两值是否相等
func compareValue(v1 interface{}, v2 interface{}) bool {
	equal := false

	// 处理特殊情况
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	switch v := v1.(type) {
	case []byte:
		equal = byteEquals(v, v2.([]byte))
	case decimal.Decimal:
		if newDec, ok := v2.(decimal.Decimal); ok {
			equal = v.Equal(newDec)
		} else {
			equal = false
		}
	default:
		equal = v1 == v2
	}

	return equal
}

// configPrimaryKey 配置主键设置
func (t *Table) configPrimaryKey() {
	var primarys map[int]bool
	primarys = make(map[int]bool)

	var uniques map[int]bool
	uniques = make(map[int]bool)
	for i, r := range t.Columns {
		if r.ColumnKey == "PRI" {
			primarys[i] = true
		}
		if r.ColumnKey == "UNI" {
			uniques[i] = true
		}
	}

	// newTable.tableId = tableId
	if len(primarys) > 0 {
		t.primarys = primarys
		t.hasPrimary = true
	} else if len(uniques) > 0 {
		t.primarys = uniques
		t.hasPrimary = true
	} else {
		t.hasPrimary = false
	}
}

func (p *MyBinlogParser) parseSingleEvent(e *replication.BinlogEvent) (ok bool, err error) {
	// 是否继续,默认为true
	ok = true
	finishFlag := -1

	if e.Header.LogPos > 0 {
		p.currentPosition.Pos = e.Header.LogPos
	}

	if e.Header.EventType == replication.ROTATE_EVENT {
		if event, ok := e.Event.(*replication.RotateEvent); ok {
			p.currentPosition = mysql.Position{
				Name: string(event.NextLogName),
				Pos:  uint32(event.Position)}
		}
	}

	if !p.cfg.StopNever {

		if e.Header.Timestamp > 0 {
			if p.startTimestamp > 0 && e.Header.Timestamp < p.startTimestamp {
				return
			}
			if p.stopTimestamp > 0 && e.Header.Timestamp > p.stopTimestamp {
				log.Warn("已超出结束时间")
				return false, nil
			}
		}

		p.currentTimtstamp = e.Header.Timestamp

		finishFlag = p.checkFinish(&p.currentPosition)
		if finishFlag == 1 {
			log.Warn("is finish")
			return false, nil
		}
	}

	// 只需解析GTID返回内的event
	if len(p.includeGtids) > 0 {
		switch e.Header.EventType {
		case replication.TABLE_MAP_EVENT, replication.QUERY_EVENT,
			replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2,
			replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2,
			replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			status := p.isGtidEventInGtidSet()
			if status == 1 {
				return
			} else if status == 2 {
				log.Info("已超出GTID范围,自动结束")

				if !p.cfg.StopNever {
					return false, nil
				}
			}
		}
	}

	switch event := e.Event.(type) {
	case *replication.GTIDEvent:
		if len(p.includeGtids) > 0 {
			p.gtidEvent = event
		}
		// e.Dump(os.Stdout)
		u, _ := uuid.FromBytes(event.SID)
		p.gtid = append([]byte(u.String()), []byte(fmt.Sprintf(":%d", event.GNO))...)
		// fmt.Println(p.gtid)
	case *replication.TableMapEvent:
		if !p.schemaFilter(event) {
			return
		}
		_, err = p.tableInformation(event.TableID, event.Schema, event.Table)
		if err != nil {
			return false, err
		}
	case *replication.QueryEvent:
		if p.cfg.ThreadID > 0 || p.cfg.ShowThread {
			p.currentThreadID = event.SlaveProxyID
			if p.cfg.ThreadID != p.currentThreadID {
				return
			}
		}

		// 回滚或者仅dml语句时 跳过
		if p.cfg.Flashback || !p.cfg.ParseDDL {
			return
		}

		if string(event.Query) != "BEGIN" && string(event.Query) != "COMMIT" {
			if len(event.Schema) > 0 {
				p.write(append([]byte(fmt.Sprintf("USE `%s`;\n", event.Schema)), event.Query...), e)
			}
			// changeRows++
		} else {
			// fmt.Println(string(event.Query))
			// log.Info("#start %d %d %d", e.Header.LogPos,
			//  e.Header.LogPos+e.Header.EventSize,
			//  e.Header.LogPos-e.Header.EventSize)
			// if binlog_event.query == 'BEGIN':
			//                    e_start_pos = last_pos
		}
	case *replication.RowsEvent:
		if !p.schemaFilter(event.Table) {
			return
		}
		if p.cfg.ThreadID > 0 && p.cfg.ThreadID != p.currentThreadID {
			return
		}
		table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
		if err != nil {
			return false, err
		}

		switch e.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			if _, ok := p.SqlTypes["insert"]; ok {
				if p.cfg.Flashback {
					err = p.generateDeleteSQL(table, event, e)
				} else {
					err = p.generateInsertSQL(table, event, e)
				}
				p.changeRows = p.changeRows + len(event.Rows)
			}
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			if _, ok := p.SqlTypes["delete"]; ok {
				if p.cfg.Flashback {
					err = p.generateInsertSQL(table, event, e)
				} else {
					err = p.generateDeleteSQL(table, event, e)
				}
				p.changeRows = p.changeRows + len(event.Rows)
			}
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			if _, ok := p.SqlTypes["update"]; ok {
				if p.cfg.Flashback {
					err = p.generateUpdateRollbackSQL(table, event, e)
				} else {
					err = p.generateUpdateSQL(table, event, e)
				}
				p.changeRows = p.changeRows + len(event.Rows)/2
			}
		}

		if err != nil {
			return false, err
		}

	}

	if p.cfg.MaxRows > 0 && p.changeRows >= p.cfg.MaxRows {
		log.Info("已超出最大行数")
		return false, nil
	}

	// 再次判断是否到结束位置,以免处在临界值,且无新日志时程序卡住
	if e.Header.Timestamp > 0 {
		if p.stopTimestamp > 0 && e.Header.Timestamp >= p.stopTimestamp {
			log.Warn("已超出结束时间")
			return false, nil
		}
	}

	if !p.running {
		log.Warn("进程手动中止")
		return false, nil
	}

	if finishFlag > -1 {
		return false, nil
	}

	return
}

// ParseRows 获取已解析行数
func (p *MyBinlogParser) Config() *BinlogParserConfig {
	return p.cfg
}

// ParseRows 获取已解析行数
func (p *MyBinlogParser) ParseRows() int {
	return p.changeRows
}

// Percent 获取解析百分比
func (p *MyBinlogParser) Percent() int {
	if p.cfg.StartFile != "" && p.cfg.StartFile == p.cfg.StopFile {
		if p.cfg.StopPosition > 0 {
			return (int(p.currentPosition.Pos) - p.cfg.StartPosition) * 100 / (p.cfg.StopPosition - p.cfg.StartPosition)
		}
	}

	if p.stopTimestamp > 0 {
		if p.currentTimtstamp < p.startTimestamp {
			return 0
		} else {
			return int((p.currentTimtstamp - p.startTimestamp) * 100 / (p.stopTimestamp - p.startTimestamp))
		}
	}

	if p.cfg.MaxRows > 0 {
		if p.changeRows < p.cfg.MaxRows {
			return p.changeRows / p.cfg.MaxRows * 100
		}
		return 100
	}

	return 0
}

// Archive文件压缩打包
func (p *MyBinlogParser) Archive() (fileSize int64, err error) {
	var url string
	if strings.Count(p.outputFileName, ".") > 0 {
		a := strings.Split(p.outputFileName, ".")
		url = strings.Join(a[0:len(a)-1], ".")
	} else {
		url = p.outputFileName
	}

	url = url + ".tar.gz"

	err = archiver.Archive([]string{p.outputFileName}, url)
	if err != nil {
		return 0, err
	}

	fileInfo, _ := os.Stat(url)
	//文件大小
	fileSize = fileInfo.Size()

	// 压缩完成后删除文件
	p.clear()

	log.Info("打包完成")
	return fileSize, nil
}

// getTableName 获取RowsEvent的表名
func getTableName(e *replication.RowsEvent) string {
	var build strings.Builder
	build.WriteString("`")
	build.Write(e.Table.Schema)
	build.WriteString("`.`")
	build.Write(e.Table.Table)
	build.WriteString("`")
	return build.String()
}
