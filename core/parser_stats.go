package core

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/jinzhu/now"
	log "github.com/sirupsen/logrus"
)

type SummaryStats struct {
	Inserts   int          `json:"insert"`
	Updates   int          `json:"update"`
	Deletes   int          `json:"delete"`
	Total     int          `json:"total"`
	StartTime time.Time    `json:"start_time"`
	EndTime   time.Time    `json:"end_time"`
	Tables    []TableStats `json:"tables"`
}

type TableStats struct {
	Table   string
	DB      string
	Inserts int `json:"insert"`
	Updates int `json:"update"`
	Deletes int `json:"delete"`
	Total   int `json:"total"`
}

func (s *SummaryStats) String() string {
	if len(s.Tables) > 1 {
		sort.SliceStable(s.Tables, func(cur, next int) bool {
			return s.Tables[cur].Total < s.Tables[next].Total
		})
	}
	var tableStr string
	for _, t := range s.Tables {
		tableStr += t.String()
	}

	// fmt.Sprintf("start: %v\n"+" stop: %v\n", s.StartTime.Format(time.RFC3339),
	// s.EndTime.Format(time.RFC3339)) + "\n" +
	return tableStr +
		fmt.Sprintf("\n"+`Summary: %d
	insert: %d
	update: %d
	delete: %d`+"\n", s.Total, s.Inserts, s.Updates, s.Deletes)
}

func (t *TableStats) String() string {
	return fmt.Sprintf(`%s.%s: %d [insert:%d, update:%d, delete:%d]`+"\n", t.DB, t.Table, t.Total, t.Inserts, t.Updates, t.Deletes)
}

type BinlogParserStats struct {
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

	// 表结构缓存(仅用于本地解析)
	tableCacheList map[string]*Table

	// 解析用临时变量
	currentPosition  mysql.Position // 当前binlog位置
	currentTimtstamp uint32         // 当前解析到的时间戳

	// 保存binlogs文件和position,用以计算percent
	binlogs []MasterLog

	stats       SummaryStats
	statsTables map[string]*TableStats
}

// Parser 远程解析
func (p *BinlogParserStats) ParserStats() error {
	// runtime.GOMAXPROCS(runtime.NumCPU())

	if p.cfg.Host == "" && p.cfg.StartFile != "" {
		_, err := os.Stat(p.cfg.StartFile)
		if err != nil {
			return fmt.Errorf("open start_file: %w", err)
		}
		return p.parserFile()
	}

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
			return fmt.Errorf("create output file: %w", err)
		}

		p.bufferWriter = bufio.NewWriter(p.outputFile)
	}

	cfg := replication.BinlogSyncerConfig{
		ServerID: 2000000211,
		Flavor:   p.cfg.Flavor,

		Host:       p.cfg.Host,
		Port:       p.cfg.Port,
		User:       p.cfg.User,
		Password:   p.cfg.Password,
		UseDecimal: true,
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
		return fmt.Errorf("Start sync: %w", err)
	}

	var finishError error
FOR:
	for {
		e, err := s.GetEvent(p.ctx)
		if err != nil {
			if err == context.DeadlineExceeded {
				log.Warnf("Waiting for timeout(10s), no new event is generated, automatically stop: %v\n", err)
				// err = fmt.Errorf("Waiting for timeout(10s), no new event is generated, automatically stop: %v\n", err)
				err = nil
			} else {
				log.Errorf("Get event error: %v\n", err)
			}
			finishError = fmt.Errorf("get binlog event: %w", err)
			break FOR
		}

		select {
		case <-p.ctx.Done():
			finishError = context.Canceled
			break FOR
		default:
			ok, err := p.parseSingleEvent(e)
			if err != nil {
				finishError = err
				break FOR
			}
			if !ok {
				break FOR
			}
		}
	}

	p.running = false
	return finishError
}

// checkFinish 检查是否需要结束循环
func (p *BinlogParserStats) checkFinish(currentPosition *mysql.Position) int {
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
		if currentPosition.Pos > uint32(p.cfg.StopPosition) {
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
		}).Info(stopMsg)
	}
	return returnValue
}

// clear 压缩完成或者没有数据时删除文件
func (p *BinlogParserStats) clear() {
	err := os.Remove(p.outputFileName)
	if err != nil {
		log.Error(err)
		log.Errorf("删除文件失败! %s", p.outputFileName)
	}
}

func (p *BinlogParserStats) Stop() {
	log.Warn("Process killed")
	if p.cancelFn != nil {
		p.cancelFn()
	}
	p.running = false
}

func (p *BinlogParserStats) writeString(str string) {
	if len(p.outputFileName) > 0 {
		p.bufferWriter.WriteString(str)
	} else {
		fmt.Print(str)
	}
}

// NewBinlogParserStats binlog解析器
func NewBinlogParserStats(cfg *BinlogParserConfig) (*BinlogParserStats, error) {

	p := new(BinlogParserStats)

	p.allTables = make(map[uint64]*Table)

	cfg.beginTime = time.Now().Unix()
	p.cfg = cfg

	p.OnlyDatabases = make(map[string]bool)
	// [table_name] = db_name
	p.OnlyTables = make(map[string]string)
	p.SqlTypes = make(map[string]bool)

	p.startFile = cfg.StartFile
	p.stopFile = cfg.StopFile

	if cfg.OutputFileStr != "" {
		p.outputFileName = cfg.OutputFileStr
	} else {
		p.outputFile = os.Stdout
	}

	// 本地解析模式,host为空,表名为SQL文件
	if p.cfg.Host == "" {
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

	p.ctx, p.cancelFn = context.WithCancel(context.Background())

	p.stats = SummaryStats{}
	p.statsTables = make(map[string]*TableStats)
	return p, nil
}

// parserInit 解析服务初始化
func (p *BinlogParserStats) parserInit() error {

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

		binlogs := p.autoParseBinlogPosition()
		if len(binlogs) == 0 {
			p.checkError(errors.New("无法获取master binlog"))
		}

		p.startFile = binlogs[0].Name

		if p.startTimestamp > 0 || p.stopTimestamp > 0 {
			for _, masterLog := range binlogs {
				timestamp, err := p.getBinlogFirstTimestamp(masterLog.Name)
				p.checkError(err)

				log.WithFields(log.Fields{
					"起始时间":       time.Unix(int64(timestamp), 0).Format(TimeFormat),
					"binlogFile": masterLog.Name,
				}).Info("binlog信息")

				if timestamp <= p.startTimestamp {
					p.startFile = masterLog.Name
				}

				if p.stopFile == "" && p.stopTimestamp > 0 && timestamp > p.stopTimestamp {
					p.stopFile = masterLog.Name
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
		for _, db := range strings.Split(p.cfg.Databases, ",") {
			db = strings.ToLower(strings.Trim(db, " `"))
			p.OnlyDatabases[db] = true
		}
	}

	if len(p.cfg.Tables) > 0 {
		for _, s := range strings.Split(p.cfg.Tables, ",") {
			if strings.Contains(s, ".") {
				names := strings.SplitN(s, ".", 2)
				db, table := names[0], names[1]
				db = strings.ToLower(strings.Trim(db, " `"))
				table = strings.ToLower(strings.Trim(table, " `"))
				p.OnlyTables[table] = db
			} else {
				key := strings.ToLower(strings.Trim(s, " `"))
				p.OnlyTables[key] = ""
			}

		}
	}

	// log.Infof("dbs: %#v", p.OnlyDatabases)
	// log.Infof("tbls: %#v", p.OnlyTables)

	return nil
}

// getBinlogFirstTimestamp 获取binlog文件第一个时间戳
func (p *BinlogParserStats) getBinlogFirstTimestamp(file string) (uint32, error) {

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

func (p *BinlogParserStats) checkError(e error) {
	if e != nil {
		log.Error(e)
		panic(e)
	}
}

func (p *BinlogParserStats) mysqlMasterStatus() (*MasterStatus, error) {

	defer timeTrack(time.Now(), "mysqlMasterStatus")

	r := MasterStatus{}

	p.db.Raw("SHOW MASTER STATUS").Scan(&r)

	return &r, nil
}

func (p *BinlogParserStats) autoParseBinlogPosition() []MasterLog {

	if p.binlogs != nil {
		return p.binlogs
	}
	var binlogIndex []MasterLog
	p.db.Raw("SHOW MASTER LOGS").Scan(&binlogIndex)

	p.binlogs = binlogIndex
	return binlogIndex
}

func getDBTableKey(db, table string) string {
	return fmt.Sprintf("`%s`.`%s`", strings.ToLower(db), strings.ToLower(table))
}

func (p *BinlogParserStats) parseSingleEvent(e *replication.BinlogEvent) (ok bool, err error) {
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

	if e.Header.Timestamp > 0 {
		if p.startTimestamp > 0 && e.Header.Timestamp < p.startTimestamp {
			return
		}

		if p.stats.StartTime.IsZero() {
			p.stats.StartTime = time.Unix(int64(e.Header.Timestamp), 0)
		}
		p.stats.EndTime = time.Unix(int64(e.Header.Timestamp), 0)

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

	switch event := e.Event.(type) {
	case *replication.RowsEvent:
		key := getDBTableKey(string(event.Table.Schema), string(event.Table.Table))
		if _, ok := p.statsTables[key]; !ok {
			p.statsTables[key] = &TableStats{
				DB:    string(event.Table.Schema),
				Table: string(event.Table.Table),
			}
		}
		currentTable := p.statsTables[key]

		switch e.Header.EventType {
		case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
			currentTable.Inserts += len(event.Rows)
			currentTable.Total += len(event.Rows)
		case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
			currentTable.Deletes += len(event.Rows)
			currentTable.Total += len(event.Rows)
		case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
			currentTable.Updates += len(event.Rows) / 2
			currentTable.Total += len(event.Rows)
		}
	}

	// 再次判断是否到结束位置,以免处在临界值,且无新日志时程序卡住
	if e.Header.Timestamp > 0 {
		if p.stopTimestamp > 0 && e.Header.Timestamp > p.stopTimestamp {
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

func (p *BinlogParserStats) parserFile() error {
	defer timeTrack(time.Now(), "parserFile")

	defer func() {
		if p.outputFile != nil {
			p.outputFile.Close()
		}
	}()

	var err error

	p.running = true

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

	b := replication.NewBinlogParser()
	b.SetUseDecimal(true)

	p.currentPosition = mysql.Position{
		Name: p.startFile,
		Pos:  uint32(p.cfg.StartPosition),
	}

	f := func(e *replication.BinlogEvent) error {

		ok, err := p.parseSingleEvent(e)
		if err != nil {
			return err
		}
		if !ok {
			b.Stop()
		}

		// 再次判断是否到结束位置,以免处在临界值,且无新日志时程序卡住
		if e.Header.Timestamp > 0 {
			if p.stopTimestamp > 0 && e.Header.Timestamp >= p.stopTimestamp {
				log.Warn("已超出结束时间")

				b.Stop()
				return nil
			}
		}

		return nil
	}

	err = b.ParseFile(p.startFile, int64(p.cfg.StartPosition), f)

	if err != nil {
		println(err.Error())
	}

	p.stats.Tables = make([]TableStats, 0, len(p.statsTables))
	for _, t := range p.statsTables {
		p.stats.Inserts += t.Inserts
		p.stats.Updates += t.Updates
		p.stats.Deletes += t.Deletes
		p.stats.Total += t.Total
		p.stats.Tables = append(p.stats.Tables, *t)
	}

	p.writeString(p.stats.String())

	return nil
}
