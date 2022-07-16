/*
Copyright © 2020 NAME HERE <EMAIL ADDRESS>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package cmd

import (
	"math"

	"github.com/hanchuanchuan/bingo2sql/core"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// runServer = flagBoolean(flag,, "server", "s", false, "启动API服务")
// summary = flagBoolean(flag, "summary", "S", false, "统计binlog文件中的DML次数等信息")
// configFile = flag.StringP("config", "c", "config.ini", "以服务方式启动时需指定配置文件")

// rootCmd represents the base command when called without any subcommands
var summaryCmd = &cobra.Command{
	Use:   "summary",
	Short: "summary binlog events",
	Long:  `统计binlog文件中的DML次数等信息`,
	Run: func(cmd *cobra.Command, args []string) {
		if cfg.Debug {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.ErrorLevel)
		}

		// thread_id溢出处理
		if threadID > math.MaxUint32 {
			cfg.ThreadID = uint32(threadID % (1 << 32))
		} else {
			cfg.ThreadID = uint32(threadID)
		}

		log.Errorf("cfg: %#v", cfg)
		if p, err := core.NewBinlogParser(&cfg); err != nil {
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

	},
}

func init() {
	rootCmd.AddCommand(summaryCmd)

	flag := summaryCmd.Flags()
	flag.SortFlags = false

	// runServer = flagBoolean(flag,, "server", "s", false, "启动API服务")
	// summary = flagBoolean(flag, "summary", "S", false, "统计binlog文件中的DML次数等信息")
	// configFile = flag.StringP("config", "c", "config.ini", "以服务方式启动时需指定配置文件")
}
