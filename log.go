package bingo2sql

import (
	"fmt"
	"path"
	"runtime"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	BuildTS    = ""
	GitHash    = ""
	GitBranch  = ""
	GoVersion  = ""
	GitVersion = ""
)

func init() {

	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	// log.SetFormatter(&log.TextFormatter{
	// 	DisableColors: true,
	// 	FullTimestamp: true,
	// })

	// log.SetReportCaller(true)

	// 输出文件名和行号
	log.AddHook(&contextHook{})
}

// modifyHook injects file name and line pos into log entry.
type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 4)
	cnt := runtime.Callers(6, pc)
	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

// PrintInfo prints the  version information.
func PrintVersion() {
	log.Infof("Version: %s", GitVersion)
	log.Infof("Git Commit Hash: %s", GitHash)
	log.Infof("Git Branch: %s", GitBranch)
	log.Infof("UTC Build Time:  %s", BuildTS)
	log.Infof("Go Version:  %s", GoVersion)
}

// GetInfo returns the git hash and build time of this -server binary.
func GetInfo() string {
	return fmt.Sprintf(
		"Version: %v\n"+
			"Git Commit Hash: %s\n"+
			"Git Branch: %s\n"+
			"UTC Build Time: %s\n"+
			"Go Version: %s",
		GitVersion,
		GitHash,
		GitBranch,
		BuildTS,
		GoVersion,
	)
}
