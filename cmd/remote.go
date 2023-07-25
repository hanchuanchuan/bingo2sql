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
	"math"

	"github.com/hanchuanchuan/bingo2sql/core"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var remoteCmd = &cobra.Command{
	Use:   "remote",
	Short: "远程解析",
	Long:  `远程解析需要指定数据库地址/端口等信息`,
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

func init() {
	rootCmd.AddCommand(remoteCmd)

	flag := remoteCmd.Flags()
	flag.SortFlags = false

	flag.StringVarP(&cfg.Host, "host", "h", "", "host")
	flag.Uint16VarP(&cfg.Port, "port", "P", 3306, "port")
	flag.StringVarP(&cfg.User, "user", "u", "", "user")
	flag.StringVarP(&cfg.Password, "password", "p", "", "password")
	flag.Bool("help", false, "help for remote")

	flag.BoolVarP(&cfg.StopNever, "stop-never", "N", false, "持续解析binlog")

	initCommonFalg(flag)
}
