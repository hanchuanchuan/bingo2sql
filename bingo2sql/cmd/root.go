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
	"fmt"
	"os"

	"github.com/hanchuanchuan/bingo2sql"
	"github.com/spf13/cobra"

	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/viper"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "bingo2sql",
	Short: "mysql binlog解析工具",
	Long:  `用于解析mysql binlog,支持正向,逆向. 可本地或远程解析.`,
	// Uncomment the following line if your bare application
	// has an action associated with it:
	Run: func(cmd *cobra.Command, args []string) {},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

var cfg *bingo2sql.BinlogParserConfig

func init() {
	cobra.OnInitialize(initConfig)

	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.bingo2sql.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
	// rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")

	cfg = &bingo2sql.BinlogParserConfig{}

	// flag := rootCmd.PersistentFlags()

	// flag.StringVar(&cfg.StartFile, "start-file", "", "start-file")
	// flag.IntVar(&cfg.StartPosition, "start-pos", 0, "start-pos")

	// flag.StringVar(&cfg.StartTime, "start-time", "", "start-time")
	// flag.StringVar(&cfg.StopTime, "stop-time", "", "stop-time")

	// flag.StringVarP(&cfg.Databases, "databases", "d", "", "数据库列表,多个时以逗号分隔")
	// flag.StringVarP(&cfg.Tables, "tables", "t", "", "表名,如果数据库为多个,则需指名表前缀,多个时以逗号分隔")

	// flag.Uint32VarP(&cfg.ThreadID, "connection-id", "C", 0, "指定线程ID")

	// flag.BoolVarP(&cfg.Flashback, "flashback", "B", false, "逆向语句")

	// flag.BoolVarP(&cfg.ParseDDL, "ddl", "", false, "解析DDL语句(仅正向SQL)")

	// flag.StringVar(&cfg.SqlType, "type", "insert,delete,update", "解析的语句类型")

	// flag.IntVar(&cfg.MaxRows, "max", 100000, "解析的最大行数,设置为0则不限制")

	// flag.StringVar(&cfg.OutputFileStr, "output", "", "output file")

	// flag.BoolVarP(&cfg.Debug, "debug", "", false, "调试模式,输出详细日志.sets log level to debug")

	// flag.BoolVarP(&cfg.RemovePrimary, "no-primary-key", "K", false, "对INSERT语句去除主键. 可选. 默认False")

	// flag.BoolVarP(&cfg.MinimalUpdate, "minimal-update", "M", false, "最小化update语句. 可选. 默认False")

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
