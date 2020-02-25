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
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// rootCmd represents the base command when called without any subcommands
var remoteCmd = &cobra.Command{
	Use:   "remote",
	Short: "命令行远程解析",
	Long:  `需要指定远程数据库信息.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {},
}

func init() {
	rootCmd.AddCommand(remoteCmd)

	flag := remoteCmd.Flags()
	flag.SortFlags = false

	// cfg = &bingo2sql.BinlogParserConfig{}

	flag.StringVarP(&cfg.Host, "host", "H", "", "host")
	flag.Uint16VarP(&cfg.Port, "port", "P", 3306, "host")
	flag.StringVarP(&cfg.User, "user", "u", "", "user")
	flag.StringVarP(&cfg.Password, "password", "p", "", "password")

	flag.StringVar(&cfg.StartFile, "start-file", "", "start-file")
	flag.StringVar(&cfg.StartFile, "stop-file", "", "stop-file")

	flag.IntVar(&cfg.StartPosition, "start-pos", 0, "start-pos")
	flag.IntVar(&cfg.StopPosition, "stop-pos", 0, "stop-pos")

	flag.StringVar(&cfg.StartTime, "start-time", "", "start-time")
	flag.StringVar(&cfg.StopTime, "stop-time", "", "stop-time")

	flag.StringVarP(&cfg.Databases, "databases", "d", "", "数据库列表,多个时以逗号分隔")
	flag.StringVarP(&cfg.Tables, "tables", "t", "", "表名,如果数据库为多个,则需指名表前缀,多个时以逗号分隔")

	initCommonFalg(flag)

	flag.BoolVarP(&cfg.StopNever, "stop-never", "N", false, "持续解析binlog")

}

func initCommonFalg(flag *pflag.FlagSet) {
	flag.Uint32VarP(&cfg.ThreadID, "connection-id", "C", 0, "指定线程ID")

	flag.BoolVarP(&cfg.Flashback, "flashback", "B", false, "逆向语句")

	flag.BoolVarP(&cfg.ParseDDL, "ddl", "", false, "解析DDL语句(仅正向SQL)")

	flag.StringVar(&cfg.SqlType, "type", "insert,delete,update", "解析的语句类型")

	flag.IntVar(&cfg.MaxRows, "max", 100000, "解析的最大行数,设置为0则不限制")

	flag.StringVar(&cfg.OutputFileStr, "output", "", "output file")

	flag.BoolVarP(&cfg.Debug, "debug", "", false, "调试模式,输出详细日志.sets log level to debug")

	flag.BoolVarP(&cfg.RemovePrimary, "no-primary-key", "K", false, "对INSERT语句去除主键. 可选. 默认False")

	flag.BoolVarP(&cfg.MinimalUpdate, "minimal-update", "M", false, "最小化update语句. 可选. 默认False")
}
