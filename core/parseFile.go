package core

import (
	"bufio"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/mholt/archiver/v3"
	log "github.com/sirupsen/logrus"
)

func (p *MyBinlogParser) parserFile() error {

	var wg sync.WaitGroup

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

	sendTime := time.Now().Add(time.Second * 5)
	sendCount := 0

	wg.Add(1)
	go p.ProcessChan(&wg)

	f := func(e *replication.BinlogEvent) error {

		ok, err := p.parseSingleEvent(e)
		if err != nil {
			return err
		}
		if !ok {
			b.Stop()
		}

		if e.Header.EventType == replication.QUERY_EVENT ||
			e.Header.EventType == replication.UPDATE_ROWS_EVENTv2 ||
			e.Header.EventType == replication.WRITE_ROWS_EVENTv2 ||
			e.Header.EventType == replication.DELETE_ROWS_EVENTv2 {

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

			if p.cfg.MaxRows > 0 && p.changeRows >= p.cfg.MaxRows {
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
		if p.changeRows > 0 {
			kwargs := map[string]interface{}{"rows": p.changeRows}
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
			kwargs = map[string]interface{}{"ok": "1", "pct": 100, "rows": p.changeRows,
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
