// go-mysqlbinlog: a simple binlog tool to sync remote MySQL binlog.
// go-mysqlbinlog supports semi-sync mode like facebook mysqlbinlog.
// see http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html
package bingo2sql

// time go run mainRemote.go -start-time="2018-09-17 00:00:00" -stop-time="2018-09-25 00:00:00" -o=1.sql
import (
    "bufio"
    "bytes"
    "context"
    "database/sql/driver"
    "fmt"
    "os"
    "reflect"
    "strconv"
    "strings"
    "sync"
    "time"

    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/mysql"
    "github.com/juju/errors"
    "github.com/mholt/archiver/v3"
    "github.com/rs/zerolog/log"
    "github.com/satori/go.uuid"
    "github.com/siddontang/go-mysql/mysql"
    "github.com/siddontang/go-mysql/replication"
)

const digits01 = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789"
const digits10 = "0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999"

type Column struct {
    gorm.Model
    ColumnName       string `gorm:"Column:COLUMN_NAME"`
    CollationName    string `gorm:"Column:COLLATION_NAME"`
    CharacterSetName string `gorm:"Column:CHARACTER_SET_NAME"`
    ColumnComment    string `gorm:"Column:COLUMN_COMMENT"`
    ColumnType       string `gorm:"Column:COLUMN_TYPE"`
    ColumnKey        string `gorm:"Column:COLUMN_KEY"`
}

type Table struct {
    tableId    uint64
    Columns    []Column
    hasPrimary bool
    primarys   map[int]bool
}

type MasterStatus struct {
    gorm.Model
    File              string `gorm:"Column:File"`
    Position          int    `gorm:"Column:Position"`
    Binlog_Do_DB      string `gorm:"Column:Binlog_Do_DB"`
    Binlog_Ignore_DB  string `gorm:"Column:Binlog_Ignore_DB"`
    Executed_Gtid_Set string `gorm:"Column:Executed_Gtid_Set"`
}

type MasterLog struct {
    gorm.Model
    Name string `gorm:"Column:Log_name"`
    Size int    `gorm:"Column:File_size"`
}

// GTID集合,可指定或排除GTID范围
type GtidSetInfo struct {
    uuid       []byte
    startSeqNo int64
    stopSeqNo  int64
}

type BinlogParserConfig struct {
    Flavor string

    InsID    int    `json:"ins_id" form:"ins_id"`
    Host     string `json:"host" form:"host"`
    Port     uint16 `json:"port" form:"port"`
    User     string `json:"user" form:"user"`
    Password string `json:"password" form:"password"`

    StartFile     string `json:"start_file" form:"start_file"`
    StopFile      string `json:"stop_file" form:"stop_file"`
    StartPosition int    `json:"start_position" form:"start_position"`
    StopPosition  int    `json:"stop_position" form:"stop_position"`

    StartTime string `json:"start_time" form:"start_time"`
    StopTime  string `json:"stop_time" form:"stop_time"`

    Flashback bool `json:"flashback" form:"flashback"`

    ParseDDL bool `json:"parse_ddl" form:"parse_ddl"`

    Databases string `json:"databases" form:"databases"`
    Tables    string `json:"tables" form:"tables"`
    SqlType   string `json:"sql_type" form:"sql_type"`
    MaxRows   int    `json:"max_rows" form:"max_rows"`

    OnlyDatabases map[string]bool
    OnlyTables    map[string]bool
    SqlTypes      map[string]bool

    OutputFileStr string

    AllColumns bool `json:"all_columns" form:"all_columns"`

    Debug bool `json:"debug" form:"debug"`

    SemiSync bool

    RawMode bool

    // websocket的用户名,用以发送进度提醒
    SocketUser string `json:"socket_user" form:"socket_user"`

    BeginTime int64

    ThreadID uint32 `json:"thread_id" form:"thread_id"`

    IncludeGtids string `json:"include_gtids" form:"include_gtids"`
}

type Parser interface {
    write([]byte, *replication.BinlogEvent)
}

type row struct {
    sql  []byte
    e    *replication.BinlogEvent
    gtid []byte
    opid string
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
}

type MyBinlogParser struct {
    baseParser

    // 记录表结构以重用
    // allTables map[uint64]*Table

    // cfg BinlogParserConfig

    masterStatus *MasterStatus

    // startFile string `json:"start_file" form:"start_file"`
    // stopFile  string `json:"stop_file" form:"stop_file"`

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
}

var (
    TimeFormat   string = "2006-01-02 15:04:05"
    TimeLocation *time.Location
)

func init() {

    TimeLocation, _ = time.LoadLocation("Asia/Shanghai")
}

func main() {

    // myConfig := BinlogParserConfig{
    //     flavor:         *flavor,
    //     startFile:      *startFile,
    //     stopFile:       *stopFile,
    //     startPosition:  *startPosition,
    //     stopPosition:   *stopPosition,
    //     startTimestamp: startTimestamp,
    //     stopTimestamp:  stopTimestamp,

    //     onlyDatabases: onlyDatabases,
    //     onlyTables:    onlyTables,
    //     sqlTypes:      sqlTypes,
    //     outputFileStr: p.cfg.OutputFileStr,

    //     flashback:  p.cfg.Flashback,
    //     onlyDml:    *onlyDml,
    //     allColumns: allColumns,
    //     semiSync:   semiSync,
    //     rawMode:    rawMode,
    //     debug:      debug,
    //     OutputFile: outputFile,
    // }

    // p = NewBinlogParser(myConfig)

    // p.Parser()

    // defer b.Close()
}

func (cfg *BinlogParserConfig) Id() string {

    s1 := strings.Replace(cfg.Host, ".", "_", -1)
    var s2 string
    if len(cfg.StartTime) > 0 {
        s2 = strings.Replace(cfg.StartTime, ".", "_", -1)
        s2 = strings.Replace(s2, " ", "_", -1)
        s2 = strings.Replace(s2, ":", "", -1)
    } else {
        s2 = strconv.FormatInt(cfg.BeginTime, 10)
    }
    return fmt.Sprintf("%s_%d_%s",
        s1,
        cfg.Port,
        s2)
}

func (p *MyBinlogParser) Parser() error {

    var wg sync.WaitGroup

    timeStart := time.Now()
    defer timeTrack(time.Now(), "Parser")

    // defer func() {
    //     if err := recover(); err != nil {
    //         if e, ok := err.(error); ok {
    //             log.Error().Err(e).Msg("binlog解析操作失败")

    //             if len(p.cfg.SocketUser) > 0 {
    //                 kwargs := map[string]interface{}{"error": e.Error()}
    //                 sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度",
    //                     "", kwargs)
    //             }
    //         }
    //     }
    // }()

    var err error

    p.running = true

    p.db, err = p.getDB()
    if err != nil{
        return err
    }
    defer p.db.Close()

    log.Debug().Str("timeTrack", time.Since(timeStart).String()).Msg("用时")

    if len(p.outputFileName) > 0 {

        if Exists(p.outputFileName) {
            // p.checkError(errors.New("指定的文件已存在!"))
            // 不作追加,改为文件存在时清除内容,O_APPEND
            p.outputFile, err = os.OpenFile(p.outputFileName,
                os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
        } else {
            p.outputFile, err = os.Create(p.outputFileName)
        }
        if err != nil{
            return err
        }

        // outputFile, err := os.OpenFile(outputFileStr, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
        // if err != nil {
        //  log.Info().Msg(index, "open file failed.", err.Error())
        //  break
        // }

        p.bufferWriter = bufio.NewWriter(p.outputFile)
    }

    log.Debug().Str("timeTrack", time.Since(timeStart).String()).Msg("用时")

    endPos := mysql.Position{p.masterStatus.File, uint32(p.masterStatus.Position)}

    cfg := replication.BinlogSyncerConfig{
        ServerID: 2000000111,
        Flavor:   p.cfg.Flavor,

        Host:            p.cfg.Host,
        Port:            p.cfg.Port,
        User:            p.cfg.User,
        Password:        p.cfg.Password,
        RawModeEnabled:  p.cfg.RawMode,
        SemiSyncEnabled: p.cfg.SemiSync,
    }

    b := replication.NewBinlogSyncer(cfg)
    defer b.Close()
    pos := mysql.Position{p.startFile, uint32(p.cfg.StartPosition)}

    log.Debug().Str("timeTrack", time.Since(timeStart).String()).Msg("用时")

    s, err := b.StartSync(pos)
    if err != nil {
        log.Info().Msgf("Start sync error: %v\n", errors.ErrorStack(err))
        return nil
    }

    i := 0

    currentPosition := pos

    sendTime := time.Now().Add(time.Second * 5)
    sendCount := 0
    var currentThreadID uint32

    log.Debug().Str("timeTrack", time.Since(timeStart).String()).Msg("用时")

    wg.Add(1)
    go p.ProcessChan(&wg)

    // fmt.Println(endPos)
    for {
        ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
        defer cancel()
        // e, err := s.GetEvent(context.Background())
        e, err := s.GetEvent(ctx)
        if err != nil {
            // Try to output all left events
            // events := s.DumpEvents()
            // for _, e := range events {
            //  // e.Dump(os.Stdout)
            //  log.Info().Msg("===============")
            //  log.Info().Msg(e.Header.EventType)
            // }
            log.Info().Msgf("Get event error: %v\n", errors.ErrorStack(err))
            break
        }

        if e.Header.LogPos > 0 {
            currentPosition.Pos = e.Header.LogPos
        }

        if e.Header.EventType == replication.ROTATE_EVENT {
            if event, ok := e.Event.(*replication.RotateEvent); ok {
                currentPosition = mysql.Position{string(event.NextLogName),
                    uint32(event.Position)}
            }
        }

        // fmt.Println(currentPosition)

        if e.Header.Timestamp > 0 {
            if p.startTimestamp > 0 && e.Header.Timestamp < p.startTimestamp {
                continue // goto CHECK_STOP
            }
            if p.stopTimestamp > 0 && e.Header.Timestamp > p.stopTimestamp {
                log.Warn().Msg("已超出结束时间")
                break
            }
        }

        if currentPosition.Name > p.stopFile ||
            (p.cfg.StopPosition > 0 && currentPosition.Name == p.stopFile &&
                currentPosition.Pos > uint32(p.cfg.StopPosition)) ||
            (currentPosition.Name == p.masterStatus.File &&
                currentPosition.Pos > uint32(p.masterStatus.Position)) {
            // 满足以下任一条件时结束循环
            // * binlog文件超出指定结束文件
            // * 超出指定结束文件和位置
            // * 超出当前最新binlog位置
            log.Warn().
                Str("当前文件", currentPosition.Name).
                Str("结束文件", p.stopFile).
                Int("当前位置", int(currentPosition.Pos)).
                Int("结束位置", int(p.cfg.StopPosition)).
                Msg("已超出指定位置")
            break
        }

        if e.Header.EventType == replication.GTID_EVENT {
            if event, ok := e.Event.(*replication.GTIDEvent); ok {

                if len(p.includeGtids) > 0 {
                    p.gtidEvent = event
                }

                // e.Dump(os.Stdout)
                u, _ := uuid.FromBytes(event.SID)
                p.gtid = append([]byte(u.String()), []byte(fmt.Sprintf(":%d", event.GNO))...)
                // fmt.Println(p.gtid)
            }
        }

        if (e.Header.EventType == replication.TABLE_MAP_EVENT ||
            e.Header.EventType == replication.QUERY_EVENT ||
            e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 ||
            e.Header.EventType == replication.WRITE_ROWS_EVENTv2 ||
            e.Header.EventType == replication.DELETE_ROWS_EVENTv2) &&
            len(p.includeGtids) > 0 {

            status := p.isGtidEventInGtidSet()
            if status == 1 {
                continue // goto CHECK_STOP
            } else if status == 2 {
                log.Info().Msg("已超出GTID范围,自动结束")
                break
            }
        }

        if e.Header.EventType == replication.TABLE_MAP_EVENT {
            if event, ok := e.Event.(*replication.TableMapEvent); ok {
                if !p.schemaFilter(event) {
                    continue // goto CHECK_STOP
                }

                _, err = p.tableInformation(event.TableID, event.Schema, event.Table)
                if err != nil{
                    return err
                }
            }
        } else if e.Header.EventType == replication.QUERY_EVENT {
            // 回滚或者仅dml语句时 跳过
            if (p.cfg.Flashback || !p.cfg.ParseDDL) && p.cfg.ThreadID == 0 {
                continue // goto CHECK_STOP
            }

            if event, ok := e.Event.(*replication.QueryEvent); ok {
                // if !strings.Contains(string(event.Query), "BEGIN") {

                if p.cfg.ThreadID > 0 {
                    currentThreadID = event.SlaveProxyID
                }

                if p.cfg.Flashback || !p.cfg.ParseDDL {
                    continue // goto CHECK_STOP
                }

                if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
                    continue // goto CHECK_STOP
                }

                if string(event.Query) != "BEGIN" && string(event.Query) != "COMMIT" {

                    if len(string(event.Schema)) > 0 {
                        p.write(append([]byte(fmt.Sprintf("USE `%s`;\n", event.Schema)), event.Query...), e)
                    }

                    i++
                } else {
                    // fmt.Println(string(event.Query))
                    // log.Info().Msg("#start %d %d %d", e.Header.LogPos,
                    //  e.Header.LogPos+e.Header.EventSize,
                    //  e.Header.LogPos-e.Header.EventSize)
                    // if binlog_event.query == 'BEGIN':
                    //                    e_start_pos = last_pos
                }
            }
        } else if _, ok := p.cfg.SqlTypes["insert"]; ok &&
            e.Header.EventType == replication.WRITE_ROWS_EVENTv2 {
            if event, ok := e.Event.(*replication.RowsEvent); ok {
                if !p.schemaFilter(event.Table) {
                    continue // goto CHECK_STOP
                }

                if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
                    continue // goto CHECK_STOP
                }

                table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
                if err != nil{
                    return err
                }

                if p.cfg.Flashback {
                    _, err = p.generateDeleteSql(table, event, e)
                } else {
                    _, err = p.generateInsertSql(table, event, e)
                }
                if err != nil{
                    return err
                }
                i = i + len(event.Rows)
                // log.Info().Int("i", i).Int("len行数", len(event.Rows)).Msg("解析行数")
            }
        } else if _, ok := p.cfg.SqlTypes["delete"]; ok &&
            e.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
            if event, ok := e.Event.(*replication.RowsEvent); ok {
                if !p.schemaFilter(event.Table) {
                    continue // goto CHECK_STOP
                }

                if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
                    continue // goto CHECK_STOP
                }

                table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
                p.checkError(err)

                if p.cfg.Flashback {
                    _, err = p.generateInsertSql(table, event, e)
                } else {
                    _, err = p.generateDeleteSql(table, event, e)
                }
                if err != nil{
                    return err
                }
                i = i + len(event.Rows)
                // log.Info().Int("i", i).Int("len行数", len(event.Rows)).Msg("解析行数")
            }
        } else if _, ok := p.cfg.SqlTypes["update"]; ok &&
            e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
            if event, ok := e.Event.(*replication.RowsEvent); ok {
                if !p.schemaFilter(event.Table) {
                    continue // goto CHECK_STOP
                }

                if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
                    continue // goto CHECK_STOP
                }

                table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
                if err != nil{
                    return err
                }
                _, err = p.generateUpdateSql(table, event, e)
                if err != nil{
                    return err
                }
                i = i + len(event.Rows)/2
            }
        }

        if e.Header.EventType == replication.QUERY_EVENT ||
            e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 ||
            e.Header.EventType == replication.WRITE_ROWS_EVENTv2 ||
            e.Header.EventType == replication.DELETE_ROWS_EVENTv2 {

            if len(p.cfg.SocketUser) > 0 {
                // 如果指定了websocket用户,就每5s发送一次进度通知
                if i > sendCount && time.Now().After(sendTime) {
                    sendCount = i
                    sendTime = time.Now().Add(time.Second * 5)

                    kwargs := map[string]interface{}{"rows": i}
                    if p.stopTimestamp > 0 && p.startTimestamp > 0 && p.stopTimestamp > p.startTimestamp {
                        kwargs["pct"] = (e.Header.Timestamp - p.startTimestamp) * 100 / (p.stopTimestamp - p.startTimestamp)
                    }
                    go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度", "", kwargs)

                }
            }

            if p.cfg.MaxRows > 0 && i >= p.cfg.MaxRows {
                log.Info().Msg("已超出最大行数")
                break
            }
        }

        // CHECK_STOP:
        if currentPosition.Compare(endPos) > -1 {
            log.Warn().
                Str("当前文件", currentPosition.Name).
                Str("结束文件", endPos.Name).
                Int("当前位置", int(currentPosition.Pos)).
                Int("结束位置", int(endPos.Pos)).
                Msg("已超出指定位置")
            // log.Info().Msg("操作完成", currentPosition, endPos)
            break
        }

        // 再次判断是否到结束位置,以免处在临界值,且无新日志时程序卡住
        if e.Header.Timestamp > 0 {
            if p.stopTimestamp > 0 && e.Header.Timestamp >= p.stopTimestamp {
                log.Warn().Msg("已超出结束时间")
                break
            }
        }

        if currentPosition.Name > p.stopFile ||
            (p.cfg.StopPosition > 0 && currentPosition.Name == p.stopFile &&
                currentPosition.Pos >= uint32(p.cfg.StopPosition)) ||
            (currentPosition.Name == p.masterStatus.File &&
                currentPosition.Pos >= uint32(p.masterStatus.Position)) {
            // 满足以下任一条件时结束循环
            // * binlog文件超出指定结束文件
            // * 超出指定结束文件和位置
            // * 超出当前最新binlog位置
            log.Warn().
                Str("当前文件", currentPosition.Name).
                Str("结束文件", p.stopFile).
                Int("当前位置", int(currentPosition.Pos)).
                Int("结束位置", int(p.cfg.StopPosition)).
                Msg("已超出指定位置")
            break
        }

        if !p.running {
            fmt.Println("进程手动中止")
            break
        }
    }

    log.Info().Msg("操作完成")

    // if len(p.outputFileName) > 0 {
    //     p.bufferWriter.Flush()
    // }

    close(p.ch)

    wg.Wait()

    if len(p.cfg.SocketUser) > 0 {
        p.outputFile.Close()

        if i > 0 {

            kwargs := map[string]interface{}{"rows": i}
            kwargs["pct"] = 99

            go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度", "", kwargs)

            var url string
            if strings.Count(p.outputFileName, ".") > 0 {
                a := strings.Split(p.outputFileName, ".")
                url = strings.Join(a[0:len(a)-1], ".")
            } else {
                url = p.outputFileName
            }

            url = url + ".tar.gz"



            // err := archiver.TarGz.Make(url, []string{p.outputFileName})
            // err := archiver.TarGz.Archiver(
            //     []string{p.outputFileName},
            //     url)
            err := archiver.Archive([]string{p.outputFileName}, url)
            if err != nil{
                return err
            }

            fileInfo, _ := os.Stat(url)
            //文件大小
            filesize := fileInfo.Size()

            // 压缩完成后删除文件
            p.clear()

            log.Info().Msg("压缩完成")
            kwargs = map[string]interface{}{"ok": "1", "pct": 100, "rows": i,
                "url": "/go/download/" + strings.Replace(url, "../", "", -1), "size": filesize}
            go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度",
                "", kwargs)
        } else {

            // 没有数据时清除文件
            p.clear()

            kwargs := map[string]interface{}{"ok": "1", "size": 0, "pct": 100, "rows": 0}

            go sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度", "", kwargs)
        }
    }

    log.Info().Msg("操作结束")
    return nil
}

func (p *MyBinlogParser) clear() {
    // 压缩完成或者没有数据时删除文件
    err := os.Remove(p.outputFileName)
    if err != nil {
        log.Error().Err(err).Str("file", p.outputFileName).Msg("文件删除失败")
    }
}

func StringSliceEqual(a, b []byte) bool {
    if len(a) != len(b) {
        return false
    }

    if (a == nil) != (b == nil) {
        return false
    }

    for i, v := range a {
        if v != b[i] {
            return false
        }
    }

    return true
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

func (p *baseParser) write(b []byte, binEvent *replication.BinlogEvent) {

    // log.Debug().Str("write", "baseParser").Msg("write调用")

    // switch x := p.write1.(type) {
    // case *MyBinlogParser2:
    //     x.write(b, binEvent)
    // case *MyBinlogParser:
    //     x.write(b, binEvent)
    // }

    switch x := p.write1.(type) {
    case *MyBinlogParser:
        p.ch <- &row{sql: b, e: binEvent, gtid: x.gtid}
    // case *MyBinlogParser2:
    //     p.ch <- &row{sql: b, e: binEvent, opid: x.currentBackupInfo.OPID}
    }
}

func (p *MyBinlogParser) myWrite(b []byte, binEvent *replication.BinlogEvent, gtid []byte) {

    if len(p.lastGtid) == 0 {
        p.lastGtid = gtid

        p.write2(append([]byte("# "), append(gtid, "\n"...)...))

    } else if !reflect.DeepEqual(gtid, p.lastGtid) {
        p.lastGtid = gtid
        p.write2(append([]byte("\n# "), append(gtid, "\n"...)...))
    }

    if p.lastTimestamp != binEvent.Header.Timestamp {
        timeinfo := fmt.Sprintf("; # %s\n",
            time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
        b = append(b, timeinfo...)

        p.lastTimestamp = binEvent.Header.Timestamp
    } else {
        b = append(b, ";\n"...)
    }

    p.write2(b)
}

func (p *MyBinlogParser) write2(b []byte) {

    if len(p.outputFileName) > 0 {
        p.bufferWriter.Write(b)
    } else {
        fmt.Print(string(b))
    }
}

// NewBinlogParser binlog解析器
func NewBinlogParser(cfg *BinlogParserConfig) (*MyBinlogParser,error) {

    p := new(MyBinlogParser)

    p.allTables = make(map[uint64]*Table)
    p.jumpGtids = make(map[*GtidSetInfo]bool)

    p.cfg = cfg

    p.startFile = cfg.StartFile
    p.stopFile = cfg.StopFile

    var err error
    p.db, err = p.getDB()
    if err !=nil{
        return nil,err
    }
    defer p.db.Close()

    if len(p.cfg.StartTime) == 0 {
        if len(p.startFile) == 0 {
           return nil,errors.New("未指定binlog解析起点")
        }
    }

    p.parseGtidSets()

    if len(cfg.SocketUser) > 0 || cfg.OutputFileStr == ""{
        var fileName []string
        fileName = append(fileName, strings.Replace(cfg.Host, ".", "_", -1))
        fileName = append(fileName, strconv.Itoa(int(cfg.Port)))
        if cfg.Databases != "" {
            fileName = append(fileName, cfg.Databases)
        }
        if cfg.Tables != "" {
            fileName = append(fileName, cfg.Tables)
        }

        fileName = append(fileName, time.Now().Format("20060102_1504"))

        if cfg.Flashback {
            fileName = append(fileName, "rollback")
        }

        // p.outputFileName = fmt.Sprintf("files/%d.sql", time.Now().Unix())
        p.outputFileName = fmt.Sprintf("files/%s.sql", strings.Join(fileName, "_"))
    } else {
            p.outputFileName = cfg.OutputFileStr
    }

    err = p.parserInit()
    if err !=nil{
        return nil,err
    }

    if p.cfg.Debug {
        p.db.LogMode(true)
    }

    p.masterStatus, err = p.mysqlMasterStatus()
    if err !=nil{
        return nil,err
    }

    p.write1 = p

    p.ch = make(chan *row, 50)

    return p,nil
}

func (p *MyBinlogParser) ProcessChan(wg *sync.WaitGroup) {
    for {
        r := <-p.ch
        if r == nil {
            log.Info().Int("剩余ch", len(p.ch)).Int("cap ch", cap(p.ch)).Msg("通道关闭,跳出循环")
            log.Print(len(p.outputFileName))
            if len(p.outputFileName) > 0 {
                p.bufferWriter.Flush()
            }
            wg.Done()
            break
        }
        p.myWrite(r.sql, r.e, r.gtid)
    }
}

func (p *MyBinlogParser) parseGtidSets() {
    if len(p.cfg.IncludeGtids) > 0 {

        sets := strings.Split(p.cfg.IncludeGtids, ",")

        for _, s := range sets {
            g := &GtidSetInfo{}
            str := strings.Split(s, ":")

            // 把uuid逆向为16位[]byte
            u2, err := uuid.FromString(str[0])
            p.checkError(err)

            g.uuid = u2.Bytes()

            nos := strings.Split(str[1], "-")
            if len(nos) == 1 {
                n, _ := strconv.ParseInt(nos[0], 10, 64)
                g.startSeqNo = n
                g.stopSeqNo = n
            } else {
                n, _ := strconv.ParseInt(nos[0], 10, 64)
                n2, _ := strconv.ParseInt(nos[1], 10, 64)
                g.startSeqNo = n
                g.stopSeqNo = n2
            }

            p.jumpGtids[g] = false
            p.includeGtids = append(p.includeGtids, g)
            fmt.Println(*g)
        }

        log.Debug().Int("len", len(p.includeGtids)).Msg("gtid集合数量")
    }
}

func (p *baseParser) getDB() (*gorm.DB, error) {
    if p.cfg.Host == ""{
        return nil,fmt.Errorf("请指定数据库地址")
    }
    if p.cfg.Port == 0{
        return nil,fmt.Errorf("请指定数据库端口")
    }
    if p.cfg.User == ""{
        return nil,fmt.Errorf("请指定数据库用户名")
    }
    addr := fmt.Sprintf("%s:%s@tcp(%s:%d)/mysql?charset=utf8mb4&parseTime=True&loc=Local",
        p.cfg.User, p.cfg.Password, p.cfg.Host, p.cfg.Port)

    return gorm.Open("mysql", addr)
}

func (p *MyBinlogParser) parserInit() error {

    defer timeTrack(time.Now(), "parserInit")

    var err error

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
        //  log.Info().Msg(index, "open file failed.", err.Error())
        //  break
        // }
        defer p.outputFile.Close()

        p.bufferWriter = bufio.NewWriter(p.outputFile)
    }

    if p.cfg.StartTime != ""{
        p.startTimestamp, err = timeParseToUnix(p.cfg.StartTime)
        p.checkError(err)
    }
    if p.cfg.StopTime != ""{
        p.stopTimestamp, err = timeParseToUnix(p.cfg.StopTime)
        p.checkError(err)
    }

    // 如果未指定开始文件,就自动解析
    if len(p.startFile) == 0 {

        binlogIndex := p.autoParseBinlogPosition()

        p.startFile = binlogIndex[0].Name

        for _, masterLog := range binlogIndex {
            timestamp, err := p.getBinlogFirstTimestamp(masterLog.Name)
            p.checkError(err)

            log.Info().Str("binlogFile", masterLog.Name).
                Str("起始时间", time.Unix(int64(timestamp), 0).Format(TimeFormat)).
                Msg("binlog信息")

            if timestamp <= p.startTimestamp {
                p.startFile = masterLog.Name
            }

            if p.stopTimestamp > 0 && timestamp <= p.stopTimestamp {
                p.stopFile = masterLog.Name
                break
            }
        }

        if len(p.startFile) == 0 {
            p.checkError(errors.New("未能解析指定时间段的binlog文件,请检查后重试!"))
        }

        log.Info().Msgf("根据指定的时间段,解析出的开始binlog文件是:%s,结束文件是:%s\n",
            p.startFile, p.stopFile)
    }

    if len(p.stopFile) == 0 {
        p.stopFile = p.startFile
    }

    p.cfg.OnlyDatabases = make(map[string]bool)
    p.cfg.OnlyTables = make(map[string]bool)
    p.cfg.SqlTypes = make(map[string]bool)

    if len(p.cfg.SqlType) > 0 {
        for _, s := range strings.Split(p.cfg.SqlType, ",") {
            p.cfg.SqlTypes[s] = true
        }
    } else {
        p.cfg.SqlTypes["insert"] = true
        p.cfg.SqlTypes["update"] = true
        p.cfg.SqlTypes["delete"] = true
    }

    if len(p.cfg.Databases) > 0 {
        for _, s := range strings.Split(p.cfg.Databases, ",") {
            p.cfg.OnlyDatabases[s] = true
        }
    }

    if len(p.cfg.Tables) > 0 {
        for _, s := range strings.Split(p.cfg.Tables, ",") {
            p.cfg.OnlyTables[s] = true
        }
    }

    return nil
}

func Exists(filename string) bool {
    _, err := os.Stat(filename)
    return err == nil || os.IsExist(err)
}

func (p *MyBinlogParser) getBinlogFirstTimestamp(file string) (uint32, error) {

    cfg := replication.BinlogSyncerConfig{
        ServerID: 2000000110,
        Flavor:   p.cfg.Flavor,

        Host:            p.cfg.Host,
        Port:            p.cfg.Port,
        User:            p.cfg.User,
        Password:        p.cfg.Password,
        RawModeEnabled:  p.cfg.RawMode,
        SemiSyncEnabled: p.cfg.SemiSync,
    }

    b := replication.NewBinlogSyncer(cfg)

    pos := mysql.Position{file, uint32(4)}

    s, err := b.StartSync(pos)
    if err != nil {
        return 0, err
    }

    for {
        // return
        e, err := s.GetEvent(context.Background())
        if err != nil {
            b.Close()
            return 0, err
        }

        // log.Info().Msgf("事件类型:%s", e.Header.EventType)
        if e.Header.Timestamp > 0 {
            b.Close()
            return e.Header.Timestamp, nil
        }
    }

    return 0, nil
}

// 读取文件的函数调用大多数都需要检查错误，
// 使用下面这个错误检查方法可以方便一点
func check(e error) {
    if e != nil {
        panic(errors.ErrorStack(e))
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

func (p *baseParser) checkError(e error) {
    if e != nil {
        log.Error().Err(e).Msg("ERROR!!!")

        if len(p.cfg.SocketUser) > 0 {
            kwargs := map[string]interface{}{"error": e.Error()}
            sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度",
                "", kwargs)
        }
        panic(errors.ErrorStack(e))
    }
}

func (p *MyBinlogParser) schemaFilter(table *replication.TableMapEvent) bool {

    if len(p.cfg.OnlyDatabases) == 0 && len(p.cfg.OnlyTables) == 0 {
        return true
    }

    if len(p.cfg.OnlyDatabases) > 0 {
        _, ok := (p.cfg.OnlyDatabases)[string(table.Schema)]
        if !ok {
            return false
        }
    }
    if len(p.cfg.OnlyTables) > 0 {
        _, ok := (p.cfg.OnlyTables)[string(table.Table)]
        if !ok {
            return false
        }
    }

    return true
}

func (p *baseParser) generateInsertSql(t *Table, e *replication.RowsEvent,
    binEvent *replication.BinlogEvent) (string, error) {
    var buf []byte
    if len(t.Columns) < int(e.ColumnCount) {
        return "", errors.Errorf("表%s.%s缺少列!当前列数:%d,binlog的列数%d",
            e.Table.Schema, e.Table.Table, len(t.Columns), e.ColumnCount)
    }

    var columnNames []string
    // var values []byte
    c := "`%s`"
    template := "INSERT INTO `%s`.`%s`(%s) VALUES(%s)"
    for i, col := range t.Columns {
        if i < int(e.ColumnCount) {

            columnNames = append(columnNames, fmt.Sprintf(c, col.ColumnName))
        }
    }

    paramValues := strings.Repeat("?,", int(e.ColumnCount))
    paramValues = strings.TrimRight(paramValues, ",")

    sql := fmt.Sprintf(template, e.Table.Schema, e.Table.Table,
        strings.Join(columnNames, ","), paramValues)

    for _, rows := range e.Rows {

        var vv []driver.Value
        for _, d := range rows {
            vv = append(vv, d)
            // if _, ok := d.([]byte); ok {
            //  log.Info().Msgf("%s:%q\n", t.Columns[j].ColumnName, d)
            // } else {
            //  log.Info().Msgf("%s:%#v\n", t.Columns[j].ColumnName, d)
            // }
        }

        r, err := InterpolateParams(sql, vv)
        p.checkError(err)

        p.write(r, binEvent)

        // info := fmt.Sprintf(" # %s\n",
        //     time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
        // p.write([]byte(info))

        // if len(p.outputFileName) > 0 {
        //     p.bufferWriter.WriteString(r)
        //     p.bufferWriter.WriteString("\n")
        // } else {
        //     log.Info().Msg(r)
        // }
        // log.Info().Msgf(" #start %d end %d time %s",
        //  binEvent.Header.LogPos, binEvent.Header.LogPos+binEvent.Header.EventSize,
        //  time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
    }

    return string(buf), nil
}

func (p *baseParser) generateDeleteSql(t *Table, e *replication.RowsEvent,
    binEvent *replication.BinlogEvent) (string, error) {
    var buf []byte
    if len(t.Columns) < int(e.ColumnCount) {
        return "", errors.Errorf("表%s.%s缺少列!当前列数:%d,binlog的列数%d",
            e.Table.Schema, e.Table.Table, len(t.Columns), e.ColumnCount)
    }

    template := "DELETE FROM `%s`.`%s` WHERE"

    sql := fmt.Sprintf(template, e.Table.Schema, e.Table.Table)

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
                vv = append(vv, d)

                if d == nil {
                    columnNames = append(columnNames,
                        fmt.Sprintf(c_null, t.Columns[i].ColumnName))
                } else {
                    columnNames = append(columnNames,
                        fmt.Sprintf(c, t.Columns[i].ColumnName))
                }
            }

            // if _, ok := d.([]byte); ok {
            //  log.Info().Msgf("%s:%q\n", t.Columns[j].ColumnName, d)
            // } else {
            //  log.Info().Msgf("%s:%#v\n", t.Columns[j].ColumnName, d)
            // }
        }
        newSql := strings.Join([]string{sql, strings.Join(columnNames, " AND")}, "")

        r, err := InterpolateParams(newSql, vv)
        p.checkError(err)

        p.write(r, binEvent)

        // info := fmt.Sprintf("; # %s\n",
        //     time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
        // p.write([]byte(info))

        // if len(p.outputFileName) > 0 {
        //     p.bufferWriter.WriteString(r)
        //     p.bufferWriter.WriteString(";\n")
        // } else {
        //     fmt.Print(r)
        //     // log.Info().Msgf(" #start %d end %d time %s",
        //     //  binEvent.Header.LogPos, binEvent.Header.LogPos+binEvent.Header.EventSize,
        //     //  time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
        //     log.Info().Msg(";")
        // }
    }

    return string(buf), nil
}

func (p *baseParser) generateUpdateSql(t *Table, e *replication.RowsEvent,
    binEvent *replication.BinlogEvent) (string, error) {
    var buf []byte
    if len(t.Columns) < int(e.ColumnCount) {
        return "", errors.Errorf("表%s.%s缺少列!当前列数:%d,binlog的列数%d",
            e.Table.Schema, e.Table.Table, len(t.Columns), e.ColumnCount)
    }

    template := "UPDATE `%s`.`%s` SET%s WHERE"

    setValue := " `%s`=?"

    var columnNames []string
    c_null := " `%s` IS ?"
    c := " `%s`=?"

    var sets []string

    for i, col := range t.Columns {
        if i < int(e.ColumnCount) {
            sets = append(sets, fmt.Sprintf(setValue, col.ColumnName))
        }
    }

    sql := fmt.Sprintf(template, e.Table.Schema, e.Table.Table,
        strings.Join(sets, ","))

    // log.Info().Msg(sql)

    var (
        oldValues []driver.Value
        newValues []driver.Value
        newSql    string
    )
    // update时, Rows为2的倍数, 双数index为旧值,单数index为新值
    for i, rows := range e.Rows {

        if i%2 == 0 {
            // 旧值
            if !p.cfg.Flashback {
                columnNames = nil
            }

            for j, d := range rows {
                if p.cfg.Flashback {
                    newValues = append(newValues, d)
                } else {
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
            }
        } else {
            // 新值
            if p.cfg.Flashback {
                columnNames = nil
            }
            for j, d := range rows {
                if p.cfg.Flashback {

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
                    newValues = append(newValues, d)
                }
            }

            newValues = append(newValues, oldValues...)

            newSql = strings.Join([]string{sql, strings.Join(columnNames, " AND")}, "")
            // log.Info().Msg(newSql, len(newValues))
            r, err := InterpolateParams(newSql, newValues)
            p.checkError(err)

            p.write(r, binEvent)

            // info := fmt.Sprintf("; # %s\n",
            //     time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
            // p.write([]byte(info))

            // if len(p.outputFileName) > 0 {
            //     p.bufferWriter.WriteString(r)
            //     p.bufferWriter.WriteString(";\n")
            // } else {
            //     fmt.Print(r)
            //     // log.Info().Msgf(" #start %d end %d time %s",
            //     //  binEvent.Header.LogPos, binEvent.Header.LogPos+binEvent.Header.EventSize,
            //     //  time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
            //     log.Info().Msg(";")
            // }

            oldValues = nil
            newValues = nil
        }

    }

    return string(buf), nil
}

func (p *baseParser) tableInformation(tableId uint64, schema []byte, tableName []byte) (*Table, error) {

    table, ok := p.allTables[tableId]
    if ok {
        return table, nil
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
    newTable.tableId = tableId
    newTable.Columns = allRecord
    if !p.cfg.AllColumns {
        if len(primarys) > 0 {
            newTable.primarys = primarys
            newTable.hasPrimary = true
        } else if len(uniques) > 0 {
            newTable.primarys = uniques
            newTable.hasPrimary = true
        } else {
            newTable.hasPrimary = false
        }
    }

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

func InterpolateParams(query string, args []driver.Value) ([]byte, error) {
    // Number of ? should be same to len(args)
    if strings.Count(query, "?") != len(args) {
        log.Error().Str("sql", query).
            Int("需要参数", strings.Count(query, "?")).
            Int("提供参数", len(args)).Msg("sql的参数个数不匹配")

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
            log.Info().Msg("解析错误")
            return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
        }

        // 4 << 20 , 4MB
        if len(buf)+4 > 4<<20 {
            // log.Print("%T", v)
            log.Info().Msg("解析错误")
            return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
        }
    }
    if argPos != len(args) {
        // log.Print("%T", v)
        log.Info().Msg("解析错误")
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

// func ProcessWork(work_id string, db_id int, socket_user string) error {

//     dbConfig := &DBConfig{}

//     ini_cfg, err := ini.Load("../cnf/config.ini")
//     if err != nil {
//         log.Error().Err(err)
//         return err
//     }

//     err = ini_cfg.Section("DBConfig").MapTo(dbConfig)
//     if err != nil {
//         log.Error().Err(err)
//         return err
//     }

//     db, err := dbConfig.GetDB()
//     if err != nil {
//         log.Error().Err(err)
//         return err
//     }

//     backupDBConfig := &DBConfig{}
//     err = ini_cfg.Section("BackupDBConfig").MapTo(backupDBConfig)
//     if err != nil {
//         log.Error().Err(err)
//         return err
//     }

//     backupdb, err := backupDBConfig.GetDB()
//     if err != nil {
//         log.Error().Err(err)
//         return err
//     }

//     log.Info().Msg("解析开始")

//     cfg := new(BinlogParserConfig)

//     // 配置工单记录的读取链表
//     r := &incRecords{
//         work_id: work_id,
//         db_id:   db_id,

//         localdb:  db,
//         backupdb: backupdb,
//     }

//     backupInfos := NewIterator(r)

//     // 从第一条inception备份日志中获取远端服务器信息
//     first := backupInfos.First()
//     if first == nil {
//         log.Info().Msg("未找到备份信息,操作结束")
//         return errors.New("未找到备份信息,操作结束")
//     }

//     // 当实例的端口和数据库端口不一致时,表明是中间件数据库,此时替换为实际IP和PORT
//     host, port := backupInfos.getDBAddress(work_id, db_id)
//     if int(first.Port) != port {
//         cfg.Host = host
//         cfg.Port = uint16(port)
//     } else {
//         cfg.Host = first.Host
//         cfg.Port = first.Port
//     }

//     // cfg.SetRemoteDBUser(db)

//     cfg.StartFile = first.StartFile
//     cfg.StartPosition = first.StartPosition

//     cfg.SocketUser = socket_user

//     // 初始化解析类
//     p, _ := NewBinlogParser2(cfg)

//     p.localdb = db
//     p.backupdb = backupdb
//     p.backupInfos = backupInfos

//     go p.Parser2()

//     return nil

// }
