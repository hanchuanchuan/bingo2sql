// go-mysqlbinlog: a simple binlog tool to sync remote MySQL binlog.
// go-mysqlbinlog supports semi-sync mode like facebook mysqlbinlog.
// see http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html
package bingo2sql

// time go run mainRemote.go -start-time="2018-09-17 00:00:00" -stop-time="2018-09-25 00:00:00" -o=1.sql
import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/hanchuanchuan/go-mysql/mysql"
	"github.com/hanchuanchuan/go-mysql/replication"
	"github.com/mholt/archiver/v3"
	uuid "github.com/satori/go.uuid"
	log "github.com/sirupsen/logrus"
)

func (p *MyBinlogParser) parserFile() error {

	var wg sync.WaitGroup

	// timeStart := time.Now()
	defer timeTrack(time.Now(), "parserFile")

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

	// cfg := replication.BinlogSyncerConfig{
	// 	ServerID: 2000000111,
	// 	Flavor:   p.cfg.Flavor,

	// 	Host:            p.cfg.Host,
	// 	Port:            p.cfg.Port,
	// 	User:            p.cfg.User,
	// 	Password:        p.cfg.Password,
	// 	UseDecimal:      true,
	// 	RawModeEnabled:  p.cfg.RawMode,
	// 	SemiSyncEnabled: p.cfg.SemiSync,
	// }

	b := replication.NewBinlogParser()
	b.SetRawMode(p.cfg.RawMode)
	b.SetUseDecimal(true)

	currentPosition := mysql.Position{
		Name: p.startFile,
		Pos:  uint32(p.cfg.StartPosition),
	}

	i := 0

	sendTime := time.Now().Add(time.Second * 5)
	sendCount := 0
	var currentThreadID uint32

	wg.Add(1)
	go p.ProcessChan(&wg)

	finishFlag := -1
	f := func(e *replication.BinlogEvent) error {
		if finishFlag > -1 {
			// 循环已结束
			log.Info("binlog解析完成")
			b.Stop()
			return nil
		}

		if e.Header.LogPos > 0 {
			currentPosition.Pos = e.Header.LogPos
		}

		if e.Header.EventType == replication.ROTATE_EVENT {
			if event, ok := e.Event.(*replication.RotateEvent); ok {
				currentPosition = mysql.Position{
					Name: string(event.NextLogName),
					Pos:  uint32(event.Position)}
			}
		}

		if !p.cfg.StopNever {
			if e.Header.Timestamp > 0 {
				if p.startTimestamp > 0 && e.Header.Timestamp < p.startTimestamp {
					return nil
				}
				if p.stopTimestamp > 0 && e.Header.Timestamp > p.stopTimestamp {
					log.Warn("已超出结束时间")
					b.Stop()
					return nil
				}
			}

			finishFlag = p.checkFinish(&currentPosition)
			if finishFlag == 1 {
				log.Error("is finish")
				b.Stop()
				return nil
			}
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
				return nil
			} else if status == 2 {
				log.Info("已超出GTID范围,自动结束")

				if !p.cfg.StopNever {
					b.Stop()
					return nil
				}
			}
		}

		if e.Header.EventType == replication.TABLE_MAP_EVENT {
			if event, ok := e.Event.(*replication.TableMapEvent); ok {
				if !p.schemaFilter(event) {
					return nil
				}

				_, err = p.tableInformation(event.TableID, event.Schema, event.Table)
				if err != nil {
					return err
				}
			}
		} else if e.Header.EventType == replication.QUERY_EVENT {
			// 回滚或者仅dml语句时 跳过
			if (p.cfg.Flashback || !p.cfg.ParseDDL) && p.cfg.ThreadID == 0 {
				return nil
			}

			if event, ok := e.Event.(*replication.QueryEvent); ok {
				if p.cfg.ThreadID > 0 {
					currentThreadID = event.SlaveProxyID
				}

				if p.cfg.Flashback || !p.cfg.ParseDDL {
					return nil
				}

				if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
					return nil
				}

				if string(event.Query) != "BEGIN" && string(event.Query) != "COMMIT" {

					if len(string(event.Schema)) > 0 {
						p.write(append([]byte(fmt.Sprintf("USE `%s`;\n", event.Schema)), event.Query...), e)
					}

					i++
				} else {
					// fmt.Println(string(event.Query))
					// log.Info("#start %d %d %d", e.Header.LogPos,
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
					return nil
				}

				if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
					return nil
				}

				table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
				if err != nil {
					return err
				}

				if p.cfg.Flashback {
					_, err = p.generateDeleteSql(table, event, e)
				} else {
					_, err = p.generateInsertSql(table, event, e)
				}
				if err != nil {
					return err
				}
				i = i + len(event.Rows)
				// log.Info().Int("i", i).Int("len行数", len(event.Rows)).Msg("解析行数")
			}
		} else if _, ok := p.cfg.SqlTypes["delete"]; ok &&
			e.Header.EventType == replication.DELETE_ROWS_EVENTv2 {
			if event, ok := e.Event.(*replication.RowsEvent); ok {
				if !p.schemaFilter(event.Table) {
					return nil
				}

				if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
					return nil
				}

				table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
				p.checkError(err)

				if p.cfg.Flashback {
					_, err = p.generateInsertSql(table, event, e)
				} else {
					_, err = p.generateDeleteSql(table, event, e)
				}
				if err != nil {
					return err
				}
				i = i + len(event.Rows)
				// log.Info().Int("i", i).Int("len行数", len(event.Rows)).Msg("解析行数")
			}
		} else if _, ok := p.cfg.SqlTypes["update"]; ok &&
			e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
			if event, ok := e.Event.(*replication.RowsEvent); ok {
				if !p.schemaFilter(event.Table) {
					return nil
				}

				if p.cfg.ThreadID > 0 && p.cfg.ThreadID != currentThreadID {
					return nil
				}

				table, err := p.tableInformation(event.TableID, event.Table.Schema, event.Table.Table)
				if err != nil {
					return err
				}
				_, err = p.generateUpdateSql(table, event, e)
				if err != nil {
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
				log.Info("已超出最大行数")

				if !p.cfg.StopNever {
					b.Stop()
					return nil
				}
			}
		}

		// 再次判断是否到结束位置,以免处在临界值,且无新日志时程序卡住
		if e.Header.Timestamp > 0 {
			if p.stopTimestamp > 0 && e.Header.Timestamp >= p.stopTimestamp {
				log.Warn("已超出结束时间")

				b.Stop()
				return nil
			}
		}

		if !p.running {
			log.Warn("进程手动中止")
			b.Stop()
			return nil
		}

		return nil
	}

	err = b.ParseFile(p.startFile, int64(p.cfg.StartPosition), f)

	if err != nil {
		println(err.Error())
	}

	log.Info("操作完成")

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
			if err != nil {
				return err
			}

			fileInfo, _ := os.Stat(url)
			//文件大小
			filesize := fileInfo.Size()

			// 压缩完成后删除文件
			p.clear()

			log.Info("压缩完成")
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

	log.Info("操作结束")
	return nil
}
