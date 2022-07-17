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
	"github.com/hanchuanchuan/bingo2sql/core"
	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// runServer = flagBoolean(flag,, "server", "s", false, "启动API服务")
// summary = flagBoolean(flag, "summary", "S", false, "统计binlog文件中的DML次数等信息")
// configFile = flag.StringP("config", "c", "config.ini", "以服务方式启动时需指定配置文件")

// rootCmd represents the base command when called without any subcommands
var summaryCmd = &cobra.Command{
	Use:   "stats",
	Short: "stats binlog events",
	Long:  `统计binlog文件中的DML次数等信息`,
	Run: func(cmd *cobra.Command, args []string) {
		if cfg.Debug {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.ErrorLevel)
		}
		if p, err := core.NewBinlogParserStats(&cfg); err != nil {
			log.Error("binlog解析操作失败")
			log.Error(err)
			return
		} else {
			err = p.ParserStats()
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

	flag.StringVarP(&cfg.Host, "host", "h", "", "host")
	flag.Uint16VarP(&cfg.Port, "port", "P", 3306, "port")
	flag.StringVarP(&cfg.User, "user", "u", "", "user")
	flag.StringVarP(&cfg.Password, "password", "p", "", "password")
	flag.Bool("help", false, "help for remote")

	flag.StringVar(&cfg.StartFile, "start-file", "", "起始binlog文件")
	flag.StringVar(&cfg.StopFile, "stop-file", "", "结束binlog文件")
	flag.IntVar(&cfg.StartPosition, "start-pos", 0, "起始位置")
	flag.IntVar(&cfg.StopPosition, "stop-pos", 0, "结束位置")

	flag.StringVar(&cfg.StartTime, "start-time", "", "开始时间")
	flag.StringVar(&cfg.StopTime, "stop-time", "", "结束时间")

	flag.StringVarP(&cfg.Databases, "databases", "d", "", "数据库列表,多个时以逗号分隔")
	flag.StringVarP(&cfg.Tables, "tables", "t", "", "表名,如果数据库为多个,则需指名表前缀,多个时以逗号分隔")

	flag.BoolVarP(&cfg.ParseDDL, "ddl", "", false, "解析DDL语句(仅正向SQL)")

	flag.IntVar(&cfg.Threads, "threads", 64, "解析线程数,影响文件解析速度")

	flag.StringVarP(&cfg.OutputFileStr, "output", "o", "", "输出到指定文件")

	flag.StringVar(&mode, "profile-mode", "", "enable profiling mode, one of [cpu, mem, mutex, block]")

	switch mode {
	case "cpu":
		defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
	case "mem":
		defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
	case "mutex":
		defer profile.Start(profile.MutexProfile, profile.ProfilePath(".")).Stop()
	case "block":
		defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()
	}

}
