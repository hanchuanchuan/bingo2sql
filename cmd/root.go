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
	"context"
	"fmt"
	"math"
	"os"

	"github.com/hanchuanchuan/bingo2sql/core"
	"github.com/pkg/profile"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

var mode string

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "bingo2sql",
	Short: "mysql binlog解析工具",
	Long:  `用于解析mysql binlog,支持正向,逆向. 可本地或远程解析.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {
		// thread_id溢出处理
		if threadID > math.MaxUint32 {
			cfg.ThreadID = uint32(threadID % (1 << 32))
		} else {
			cfg.ThreadID = uint32(threadID)
		}
		if p, err := core.NewBinlogParser(context.Background(), &cfg); err != nil {
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

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var cfg core.BinlogParserConfig

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bingo2sql.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	cfg = core.BinlogParserConfig{
		Port: 3306,
	}

	rootCmd.PersistentFlags().BoolVarP(&cfg.Debug, "debug", "", false, "调试模式,输出详细日志.sets log level to debug")

	flag := rootCmd.Flags()
	flag.SortFlags = false

	flag.StringVarP(&cfg.Host, "host", "h", "", "host")
	flag.Uint16VarP(&cfg.Port, "port", "P", 3306, "port")
	flag.StringVarP(&cfg.User, "user", "u", "", "user")
	flag.StringVarP(&cfg.Password, "password", "p", "", "password")
	flag.Bool("help", false, "help for remote")
	initCommonFalg(flag)
	flag.BoolVarP(&cfg.StopNever, "stop-never", "N", false, "持续解析binlog")
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".bingo2sql" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".bingo2sql")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func flagBoolean(flag *pflag.FlagSet, p *bool, name string, shorthand string, defaultVal bool, usage string) {
	if !defaultVal {
		// Fix #4125, golang do not print default false value in usage, so we append it.
		usage = fmt.Sprintf("%s (default false)", usage)
	}
	flag.BoolVarP(p, name, shorthand, defaultVal, usage)
}

func initCommonFalg(flag *pflag.FlagSet) {

	flag.StringVar(&cfg.StartFile, "start-file", "", "起始binlog文件")
	flag.StringVar(&cfg.StopFile, "stop-file", "", "结束binlog文件")
	flag.IntVar(&cfg.StartPosition, "start-pos", 0, "起始位置")
	flag.IntVar(&cfg.StopPosition, "stop-pos", 0, "结束位置")

	flag.StringVar(&cfg.StartTime, "start-time", "", "开始时间")
	flag.StringVar(&cfg.StopTime, "stop-time", "", "结束时间")

	flag.StringVarP(&cfg.Databases, "databases", "d", "", "数据库列表,多个时以逗号分隔")
	flag.StringVarP(&cfg.Tables, "tables", "t", "", "表名,如果数据库为多个,则需指名表前缀,多个时以逗号分隔")

	flag.Uint64VarP(&threadID, "connection-id", "C", 0, "指定线程ID")
	flag.BoolVarP(&cfg.Flashback, "flashback", "B", false, "逆向语句")
	flag.BoolVarP(&cfg.ParseDDL, "ddl", "", false, "解析DDL语句(仅正向SQL)")

	flag.StringVar(&cfg.SqlType, "sql-type", "insert,delete,update", "解析的语句类型")

	flag.IntVar(&cfg.MaxRows, "max", 100000, "解析的最大行数,设置为0则不限制")
	flag.IntVar(&cfg.Threads, "threads", 64, "解析线程数,影响文件解析速度")

	flag.StringVarP(&cfg.OutputFileStr, "output", "o", "", "输出到指定文件")

	flag.StringVarP(&cfg.IncludeGtids, "gtid", "g", "", "GTID范围.格式为uuid:编号[-编号],多个时以逗号分隔")

	flagBoolean(flag, &cfg.RemovePrimary, "no-primary-key", "K", false, "对INSERT语句去除主键. 可选.")

	flagBoolean(flag, &cfg.MinimalUpdate, "minimal-update", "M", true, "最小化update语句. 可选.")

	flagBoolean(flag, &cfg.MinimalInsert, "minimal-insert", "I", true, "使用包含多个VALUES列表的多行语法编写INSERT语句.")

	flagBoolean(flag, &cfg.ShowGTID, "show-gtid", "", true, "显示gtid")
	flagBoolean(flag, &cfg.ShowTime, "show-time", "", true, "显示执行时间,同一时间仅显示首次")
	flagBoolean(flag, &cfg.ShowAllTime, "show-all-time", "", false, "显示每条SQL的执行时间")
	flagBoolean(flag, &cfg.ShowThread, "show-thread", "", false, "显示线程号,便于区别同一进程操作")

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
