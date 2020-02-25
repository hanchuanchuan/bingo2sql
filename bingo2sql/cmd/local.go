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
	"github.com/hanchuanchuan/bingo2sql"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var localCmd = &cobra.Command{
	Use:   "local",
	Short: "本地解析",
	Long:  `指定binlog文件和表结构文件.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		if cfg.Debug {
			log.SetLevel(log.DebugLevel)
		} else {
			log.SetLevel(log.ErrorLevel)
		}

		// // thread_id溢出处理
		// if *threadID > math.MaxUint32 {
		// 	cfg.ThreadID = uint32(*threadID % (1 << 32))
		// } else {
		// 	cfg.ThreadID = uint32(*threadID)
		// }

		if p, err := bingo2sql.NewBinlogParser(cfg); err != nil {
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
	rootCmd.AddCommand(localCmd)

	flag := localCmd.Flags()
	flag.SortFlags = false

	// cfg = &bingo2sql.BinlogParserConfig{}
	if cfg == nil {
		cfg = &bingo2sql.BinlogParserConfig{}
	}

	flag.StringVar(&cfg.StartFile, "start-file", "", "start-file")
	flag.IntVar(&cfg.StartPosition, "start-pos", 0, "start-pos")

	flag.StringVar(&cfg.StartTime, "start-time", "", "start-time")
	flag.StringVar(&cfg.StopTime, "stop-time", "", "stop-time")

	flag.StringVarP(&cfg.Databases, "databases", "d", "", "数据库列表,多个时以逗号分隔")
	flag.StringVarP(&cfg.Tables, "tables", "t", "", "表名,如果数据库为多个,则需指名表前缀,多个时以逗号分隔")

	initCommonFalg(flag)

}
