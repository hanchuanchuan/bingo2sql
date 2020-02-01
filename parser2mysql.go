// go-mysqlbinlog: a simple binlog tool to sync remote MySQL binlog.
// go-mysqlbinlog supports semi-sync mode like facebook mysqlbinlog.
// see http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html
package bingo2sql

// time go run mainRemote.go -start-time="2018-09-17 00:00:00" -stop-time="2018-09-25 00:00:00" -o=1.sql
import (
    "context"
    // "database/sql/driver"
    "fmt"
    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/mysql"
    "github.com/juju/errors"
    "github.com/rs/zerolog/log"
    "github.com/siddontang/go-mysql/mysql"
    "github.com/siddontang/go-mysql/replication"
    "strconv"
    "strings"
    "sync"
    "time"
)

type DBConfig struct { //配置文件要通过tag来指定配置文件中的名称
    Host     string
    Port     uint16
    User     string
    Password string
    DB       string
}

type MyBinlogParser2 struct {
    baseParser

    // 记录表结构以重用
    // allTables map[uint64]*Table

    // cfg BinlogParserConfig

    // startFile string
    // stopFile  string

    // cmdb数据库
    localdb *gorm.DB

    // inception备份数据库
    backupdb *gorm.DB

    // binlog数据库
    // db *gorm.DB

    // running bool

    // lastTimestamp uint32

    backupInfos *Iterator

    currentBackupInfo  *BackupInfo
    currentBackupTable string

    // 记录上次的备份表名,如果表名改变时,刷新insert缓存
    lastBackupTable string

    // 批量insert
    insertBuffer []interface{}
}

func NewBinlogParser2(cfg *BinlogParserConfig) (*MyBinlogParser2, error) {

    p := new(MyBinlogParser2)

    p.allTables = make(map[uint64]*Table)

    p.cfg = cfg

    p.cfg.Flashback = true

    p.startFile = cfg.StartFile
    p.stopFile = cfg.StopFile

    p.write1 = p

    p.ch = make(chan *row, 50)

    return p, nil
}

func (p *MyBinlogParser2) ProcessChan(wg *sync.WaitGroup) {
    for {
        r := <-p.ch
        if r == nil {
            log.Info().Int("剩余ch", len(p.ch)).Int("cap ch", cap(p.ch)).Msg("通道关闭,跳出循环")
            p.flush()
            wg.Done()
            break
        }
        p.myWrite(r.sql, r.e, r.opid)
    }
}

func (p *MyBinlogParser2) Parser2() {
    var err error
    var wg sync.WaitGroup

    p.running = true

    p.db, err = p.getDB()
    if err != nil {
        fmt.Println(p.cfg)
    }
    p.checkError(err)
    defer p.db.Close()

    defer p.localdb.Close()
    defer p.backupdb.Close()

    cfg := replication.BinlogSyncerConfig{
        ServerID: 2000000112,
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

    s, err := b.StartSync(pos)
    if err != nil {
        log.Info().Msgf("Start sync error: %v\n", errors.ErrorStack(err))
        return
    }

    i := 0

    currentPosition := pos

    // sendTime := time.Now().Add(time.Second * 5)
    // sendCount := 0

    p.currentBackupInfo = p.backupInfos.Next()
    p.setBackupTable()
    p.createTable()

    startPosition := mysql.Position{p.currentBackupInfo.StartFile, uint32(p.currentBackupInfo.StartPosition)}
    stopPosition := mysql.Position{p.currentBackupInfo.StopFile, uint32(p.currentBackupInfo.StopPosition)}
    threadid, _ := strconv.Atoi(strings.Split(p.currentBackupInfo.OPID, "_")[1])
    p.cfg.ThreadID = uint32(threadid)

    wg.Add(1)
    go p.ProcessChan(&wg)

    var currentThreadID uint32
    for {
        ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
        defer cancel()
        // e, err := s.GetEvent(context.Background())
        e, err := s.GetEvent(ctx)

        if err != nil {
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

        // 如果还没有到操作的binlog范围,跳过
        if currentPosition.Compare(startPosition) == -1 {
            continue
        }

        if e.Header.EventType == replication.TABLE_MAP_EVENT {
            if event, ok := e.Event.(*replication.TableMapEvent); ok {
                if string(event.Schema) != p.currentBackupInfo.DB ||
                    string(event.Table) != p.currentBackupInfo.Table {
                    continue
                }

                _, err = p.tableInformation(event.TableID, event.Schema, event.Table)
                p.checkError(err)
            }
        } else if e.Header.EventType == replication.QUERY_EVENT {

            if event, ok := e.Event.(*replication.QueryEvent); ok {
                currentThreadID = event.SlaveProxyID
            }

        } else if e.Header.EventType == replication.WRITE_ROWS_EVENTv2 {

            if event, ok := e.Event.(*replication.RowsEvent); ok {

                if string(event.Table.Schema) != p.currentBackupInfo.DB ||
                    string(event.Table.Table) != p.currentBackupInfo.Table {
                    continue
                }

                if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
                    continue
                }

                table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
                p.checkError(err)

                if p.cfg.Flashback {
                    _, err = p.generateDeleteSql(table, event, e)
                } else {
                    _, err = p.generateInsertSql(table, event, e)
                }
                p.checkError(err)
                i = i + len(event.Rows)
            }
        } else if e.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
            if event, ok := e.Event.(*replication.RowsEvent); ok {
                if string(event.Table.Schema) != p.currentBackupInfo.DB ||
                    string(event.Table.Table) != p.currentBackupInfo.Table {
                    continue
                }

                if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
                    continue
                }

                table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
                p.checkError(err)

                if p.cfg.Flashback {
                    _, err = p.generateInsertSql(table, event, e)
                } else {
                    _, err = p.generateDeleteSql(table, event, e)
                }
                p.checkError(err)
                i = i + len(event.Rows)
            }
        } else if e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
            if event, ok := e.Event.(*replication.RowsEvent); ok {
                if string(event.Table.Schema) != p.currentBackupInfo.DB ||
                    string(event.Table.Table) != p.currentBackupInfo.Table {
                    continue
                }

                if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
                    continue
                }

                table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
                p.checkError(err)
                _, err = p.generateUpdateSql(table, event, e)
                p.checkError(err)
                i = i + len(event.Rows)/2
            }
        }

        // if e.Header.EventType == replication.QUERY_EVENT ||
        //     e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 ||
        //     e.Header.EventType == replication.WRITE_ROWS_EVENTv2 ||
        //     e.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
        //     // fmt.Println(i)
        //     if len(p.cfg.SocketUser) > 0 {
        //         // 如果指定了websocket用户,就每5s发送一次进度通知
        //         if i > sendCount && time.Now().After(sendTime) {
        //             sendCount = i
        //             sendTime = time.Now().Add(time.Second * 5)

        //             sendMsg(p.cfg.SocketUser, "binlog_parse_progress", "binlog解析进度",
        //                 fmt.Sprintf("%d", i), nil)
        //         }
        //     }
        // }

        // 如果操作已超过binlog范围,切换到下一日志
        if currentPosition.Compare(stopPosition) > -1 {
            if p.backupInfos.HasNext() {
                p.currentBackupInfo = p.backupInfos.Next()
                p.setBackupTable()
                p.createTable()

                startPosition = mysql.Position{p.currentBackupInfo.StartFile, uint32(p.currentBackupInfo.StartPosition)}
                stopPosition = mysql.Position{p.currentBackupInfo.StopFile, uint32(p.currentBackupInfo.StopPosition)}
                threadid, _ = strconv.Atoi(strings.Split(p.currentBackupInfo.OPID, "_")[1])
                p.cfg.ThreadID = uint32(threadid)
                p.lastTimestamp = 0

                // fmt.Println("日志切换: ", startPosition, stopPosition)

            } else {
                // log.Print(currentPosition, stopPosition)
                log.Info().Msg("备份已全部解析,操作结束")
                break
            }
        }

        if !p.running {
            fmt.Println("进程手动中止")
            break
        }
    }

    close(p.ch)

    wg.Wait()

    log.Info().Int("rows", i).Msg("操作完成")

    kwargs := map[string]interface{}{"ok": "1"}

    sendMsg(p.cfg.SocketUser, "rollback_binlog_parse_complete", "binlog解析进度", "", kwargs)

}

func (c *DBConfig) GetDB() (*gorm.DB, error) {
    addr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
        c.User, c.Password, c.Host, c.Port, c.DB)

    return gorm.Open("mysql", addr)
}

func (p *MyBinlogParser2) setBackupTable() {
    dbname := fmt.Sprintf("%s_%d_%s", p.currentBackupInfo.Host,
        p.currentBackupInfo.Port,
        p.currentBackupInfo.DB)
    dbname = strings.Replace(dbname, ".", "_", -1)
    dbname = strings.Replace(dbname, "-", "_", -1)

    p.lastBackupTable = p.currentBackupTable
    p.currentBackupTable = fmt.Sprintf("`%s`.`dba_%s`", dbname, p.currentBackupInfo.Table)

    if len(p.lastBackupTable) > 0 && p.lastBackupTable != p.currentBackupTable {
        p.flush()
    }

}

// 解析的sql写入缓存,并定期入库
func (p *MyBinlogParser2) myWrite(b []byte, binEvent *replication.BinlogEvent, opid string) {

    // log.Debug().Str("write", "MyBinlogParser2").Msg("write调用")

    // sql := `insert into %s(rollback_statement,opid_time) values(?,?);`

    // if p.lastTimestamp != binEvent.Header.Timestamp {
    //     timeinfo := fmt.Sprintf("; # %s",
    //         time.Unix(int64(binEvent.Header.Timestamp), 0).Format(TimeFormat))
    //     b = append(b, timeinfo...)

    //     p.lastTimestamp = binEvent.Header.Timestamp
    // } else {
    //     b = append(b, ";"...)
    // }

    b = append(b, ";"...)

    p.insertBuffer = append(p.insertBuffer, string(b), opid)

    if len(p.insertBuffer) >= 1000 {
        p.flush()
    }
    // p.backupdb.Exec(fmt.Sprintf(sql, p.currentBackupTable),
    //     string(b), p.currentBackupInfo.OPID)
}

// flush用以写入当前insert缓存,并清空缓存.
func (p *MyBinlogParser2) flush() {

    if len(p.insertBuffer) > 0 {

        sql := `insert into %s(rollback_statement,opid_time) values%s`

        const rowSQL = "(?,?),"

        values := strings.TrimRight(strings.Repeat(rowSQL, len(p.insertBuffer)/2), ",")

        p.backupdb.Exec(fmt.Sprintf(sql, p.currentBackupTable, values),
            p.insertBuffer...)

    }
    p.insertBuffer = nil
}

func (p *MyBinlogParser2) createTable() {

    if !p.backupInfos.ExistsTable[strings.Replace(p.currentBackupTable, "`", "", -1)] {
        // if !p.backupdb.HasTable(strings.Replace(p.currentBackupTable, "`", "", -1)) {
        sql := `CREATE TABLE IF NOT EXISTS %s (
                  id bigint(20) NOT NULL AUTO_INCREMENT,
                  rollback_statement mediumtext,
                  opid_time varchar(50) DEFAULT NULL,
                  PRIMARY KEY (id)
                  );`
        p.backupdb.Exec(fmt.Sprintf(sql, p.currentBackupTable))
    } else {
        // 不再清除已有数据.
        // 该逻辑已放在Iterator的findNeedClearTable()函数中
        // sql := `DELETE FROM %s WHERE opid_time = ? ;`
        // p.backupdb.Exec(fmt.Sprintf(sql, p.currentBackupTable), p.currentBackupInfo.OPID)
    }
}

func BatchInsert(db *gorm.DB, objArr []interface{}) error {
    // If there is no data, nothing to do.
    if len(objArr) == 0 {
        return nil
    }

    mainObj := objArr[0]
    mainScope := db.NewScope(mainObj)
    mainFields := mainScope.Fields()
    quoted := make([]string, 0, len(mainFields))
    for i := range mainFields {
        // If primary key has blank value (0 for int, "" for string, nil for interface ...), skip it.
        // If field is ignore field, skip it.
        if (mainFields[i].IsPrimaryKey && mainFields[i].IsBlank) || (mainFields[i].IsIgnored) {
            continue
        }
        quoted = append(quoted, mainScope.Quote(mainFields[i].DBName))
    }

    placeholdersArr := make([]string, 0, len(objArr))

    for _, obj := range objArr {
        scope := db.NewScope(obj)
        fields := scope.Fields()
        placeholders := make([]string, 0, len(fields))
        for i := range fields {
            if (fields[i].IsPrimaryKey && fields[i].IsBlank) || (fields[i].IsIgnored) {
                continue
            }
            placeholders = append(placeholders, scope.AddToVars(fields[i].Field.Interface()))
        }
        placeholdersStr := "(" + strings.Join(placeholders, ", ") + ")"
        placeholdersArr = append(placeholdersArr, placeholdersStr)
        // add real variables for the replacement of placeholders' '?' letter later.
        mainScope.SQLVars = append(mainScope.SQLVars, scope.SQLVars...)
    }

    mainScope.Raw(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
        mainScope.QuotedTableName(),
        strings.Join(quoted, ", "),
        strings.Join(placeholdersArr, ", "),
    ))

    if _, err := mainScope.SQLDB().Exec(mainScope.SQL, mainScope.SQLVars...); err != nil {
        return err
    }
    return nil
}

func (p *MyBinlogParser2) checkError(e error) {
    if e != nil {
        log.Error().Err(e).Msg("ERROR!!!")

        if len(p.cfg.SocketUser) > 0 {
            kwargs := map[string]interface{}{"error": e.Error()}
            sendMsg(p.cfg.SocketUser, "rollback_binlog_parse_complete", "binlog解析进度",
                "", kwargs)
        }
        panic(errors.ErrorStack(e))
    }
}
