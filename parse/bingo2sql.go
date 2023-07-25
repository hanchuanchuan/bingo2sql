package parse

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/hanchuanchuan/bingo2sql/core"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"github.com/labstack/echo/v4"
	log "github.com/sirupsen/logrus"
)

// var parserProcess map[string]*parser.MyBinlogParser
var parserProcess sync.Map

type ParseInfo struct {
	ID        string `json:"id"`
	ParseRows int    `json:"parse_rows"`
	Percent   int    `json:"percent"`
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

// GetParseInfo 获取解析进程信息
func GetParseInfo(c echo.Context) error {
	id := c.Param("id")

	if len(id) == 0 {
		return c.JSON(http.StatusOK, map[string]string{
			"error": "无效参数!",
		})
	}

	if v, ok := parserProcess.Load(id); ok {
		if p, ok := v.(*core.MyBinlogParser); ok {
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

// GetAllParse 获取所有进程信息
func GetAllParse(c echo.Context) error {

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
		if p, ok := v.(*core.MyBinlogParser); ok {
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

func ParseBinlog(c echo.Context) error {

	cfg := &core.BinlogParserConfig{
		ShowGTID: true,
		ShowTime: true,

		MinimalInsert: true,
		MinimalUpdate: true,
	}

	if err := c.Bind(cfg); err != nil {
		return err
	}

	log.Infof("config: %#v", cfg)

	// 指定默认的socket用户,用来生成SQL文件
	if cfg.SocketUser == "" {
		cfg.SocketUser = "test"
	}

	// if cfg.InsID == 0 {
	// 	r := map[string]string{"error": "请指定数据库地址"}
	// 	return c.JSON(http.StatusOK, r)
	// }

	p, err := core.NewBinlogParser(context.Background(), cfg)
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
			defer func() {
				time.AfterFunc(time.Minute, func() {
					parserProcess.Delete(id)
				})
			}()
			err := p.Parser()
			if err != nil {
				log.Error(err)
			}
		}()

		r := map[string]string{"id": id}
		return c.JSON(http.StatusOK, r)
	}
}

func ParseBinlogStop(c echo.Context) error {
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
		if p, ok := v.(*core.MyBinlogParser); ok {
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

func Download(c echo.Context) error {
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
