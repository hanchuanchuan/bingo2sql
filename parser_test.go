package bingo2sql_test

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/hanchuanchuan/bingo2sql"
	"github.com/hanchuanchuan/go-mysql/client"
	"github.com/hanchuanchuan/go-mysql/mysql"
	. "github.com/pingcap/check"
	log "github.com/sirupsen/logrus"
)

// Use docker mysql to test, mysql is 3306, mariadb is 3316
var testHost = flag.String("host", "127.0.0.1", "MySQL master host")

var testOutputLogs = flag.Bool("out", false, "output binlog event")

var allTables = map[string]string{
	"test_json_v2": `CREATE TABLE IF NOT EXISTS test_json_v2 (
			id INT,
			c JSON,
			PRIMARY KEY (id)
			) ENGINE=InnoDB`,
	"test_replication": `CREATE TABLE IF NOT EXISTS test_replication (
				id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
				str VARCHAR(256),
				f FLOAT,
				d DOUBLE,
				de DECIMAL(10,2),
				i INT,
				bi BIGINT,
				e enum ("e1", "e2"),
				b BIT(8),
				y YEAR,
				da DATE,
				ts TIMESTAMP,
				dt DATETIME,
				tm TIME,
				t TEXT,
				bb BLOB,
				se SET('a', 'b', 'c'),
				PRIMARY KEY (id)
			) ENGINE=InnoDB DEFAULT CHARSET=utf8`,
	"test_json": `CREATE TABLE IF NOT EXISTS test_json (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			c1 JSON,
			c2 DECIMAL(10, 0),
			PRIMARY KEY (id)
			) ENGINE=InnoDB`,
	"test_geo": `CREATE TABLE IF NOT EXISTS test_geo (id int auto_increment primary key, g GEOMETRY)`,
	"test_parse_time": `CREATE TABLE IF NOT EXISTS test_parse_time (
		id int auto_increment primary key,
		a1 DATETIME,
		a2 DATETIME(3),
		a3 DATETIME(6),
		b1 TIMESTAMP,
		b2 TIMESTAMP(3) ,
		b3 TIMESTAMP(6))`,
	"test_simple": `CREATE TABLE IF NOT EXISTS test_simple (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			c1 varchar(100),
			c2 int,
			PRIMARY KEY (id)
			) ENGINE=InnoDB`,
	"test_long_text": `CREATE TABLE IF NOT EXISTS test_long_text (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			c1 longtext,
			PRIMARY KEY (id)
			) ENGINE=InnoDB`,
}

var (
	// 解析binlog生成的SQL文件
	binlogOutputFile string = "binlog_output.sql"
	// 本地方式解析binlog生成的SQL文件
	localOutputFile string = "binlog_output_local.sql"

	// 表结构文件. 用于本地解析
	tableSchemaFile string = "table_schema.sql"
)
var (
	defaultConfig bingo2sql.BinlogParserConfig
	localConfig   bingo2sql.BinlogParserConfig
)
var _ = Suite(&testParserSuite{})

func TestBinLogSyncer(t *testing.T) {
	TestingT(t)
}

type testParserSuite struct {
	c *client.Conn

	flavor string

	config      bingo2sql.BinlogParserConfig // 远程解析
	localConfig bingo2sql.BinlogParserConfig // 本地解析
}

func (t *testParserSuite) SetUpSuite(c *C) {
	defaultConfig = bingo2sql.BinlogParserConfig{
		Host:     *testHost,
		Port:     3306,
		User:     "test",
		Password: "test",

		StartFile: "mysql-bin.000001",

		Databases:     "test",
		OutputFileStr: binlogOutputFile,
	}

	localConfig = bingo2sql.BinlogParserConfig{
		// 通过setBinlogDir自动获取
		// StartFile: "mysql-bin.000001",

		Databases:     "test",
		Tables:        tableSchemaFile,
		OutputFileStr: localOutputFile,
	}

	t.config = defaultConfig
	t.localConfig = localConfig

	t.initTableSchema()
	t.setBinlogDir(c)

	t.createTables(c)

	log.SetLevel(log.ErrorLevel)
}

func (t *testParserSuite) TearDownSuite(c *C) {
	os.Remove(binlogOutputFile)
	os.Remove(localOutputFile)
	os.Remove(tableSchemaFile)
}

func (t *testParserSuite) SetUpTest(c *C) {
}

func (t *testParserSuite) TearDownTest(c *C) {
	t.reset()

	// if t.b != nil {
	// 	t.b.Close()
	// 	t.b = nil
	// }

	if t.c != nil {
		t.c.Close()
		t.c = nil
	}
}

func (t *testParserSuite) testExecute(c *C, query ...string) {
	for _, q := range query {
		_, err := t.c.Execute(q)
		c.Assert(err, IsNil)
	}
}

func (t *testParserSuite) setBinlogDir(c *C) {
	if t.c == nil {
		t.setupTest(c, mysql.MySQLFlavor)
	}

	result, err := t.c.Execute("show variables like 'log_bin_basename'")
	c.Assert(err, IsNil)

	basename, err := result.GetString(0, 1)
	c.Assert(err, IsNil)

	basename = strings.Replace(basename, "/data/mysql", "/Users/hanchuanchuan", 1)

	t.localConfig.StartFile = fmt.Sprintf("%s.000001", basename)
	localConfig.StartFile = fmt.Sprintf("%s.000001", basename)
	log.Infof("开始本地日志文件:%s", localConfig.StartFile)

	result, err = t.c.Execute("show master logs")
	c.Assert(err, IsNil)

	basename, err = result.GetString(0, 0)
	c.Assert(err, IsNil)
	t.config.StartFile = basename
	defaultConfig.StartFile = basename
	log.Infof("开始日志文件:%s", basename)

}

func (t *testParserSuite) SetFlashback(v bool) {
	t.config.Flashback = v
	t.localConfig.Flashback = v
}

func (t *testParserSuite) SetMinimalUpdate(v bool) {
	t.config.MinimalUpdate = v
	t.localConfig.MinimalUpdate = v
}

func (t *testParserSuite) SetRemovePrimary(v bool) {
	t.config.RemovePrimary = v
	t.localConfig.RemovePrimary = v
}

func (t *testParserSuite) SetSQLType(v string) {
	t.config.SqlType = v
	t.localConfig.SqlType = v
}

func (t *testParserSuite) SetIncludeGtids(v string) {
	t.config.IncludeGtids = v
	t.localConfig.IncludeGtids = v
}

func (t *testParserSuite) setupTest(c *C, flavor string) {
	var port uint16 = 3306
	switch flavor {
	case mysql.MariaDBFlavor:
		port = 3316
	}

	t.flavor = flavor

	var err error
	if t.c != nil {
		t.c.Close()
	}

	// db, err := sql.Open(sqlType, "user:password@tcp(127.0.0.1:3306)/database?multiStatements=true")
	// if err != nil {
	// 	log.Fatalf("open mysql err: %s", err)
	// }

	t.c, err = client.Connect(fmt.Sprintf("%s:%d", *testHost, port), "test", "test", "")
	c.Assert(err, IsNil)

	// _, err = t.c.Execute("CREATE DATABASE IF NOT EXISTS test")
	// c.Assert(err, IsNil)

	_, err = t.c.Execute("USE test")
	c.Assert(err, IsNil)

	_, err = t.c.Execute("set binlog_format = 'row'")
	c.Assert(err, IsNil)
}

func (t *testParserSuite) getThreadID(c *C) uint32 {
	result, err := t.c.Execute("select connection_id()")
	c.Assert(err, IsNil)

	threadID, err := result.GetInt(0, 0)
	c.Assert(err, IsNil)
	// log.Errorf("%#v", threadID)
	return uint32(threadID)
}

func (t *testParserSuite) getServerUUID(c *C) string {
	result, err := t.c.Execute("show variables like 'server_uuid'")
	c.Assert(err, IsNil)

	uuid, err := result.GetString(0, 1)
	c.Assert(err, IsNil)
	// log.Errorf("%#v", uuid)
	return uuid
}

// func (t *testParserSuite) testPositionSync(c *C) {
// 	//get current master binlog file and position
// 	r, err := t.c.Execute("SHOW MASTER STATUS")
// 	c.Assert(err, IsNil)
// 	binFile, _ := r.GetString(0, 0)
// 	binPos, _ := r.GetInt(0, 1)

// 	s, err := t.b.StartSync(mysql.Position{Name: binFile, Pos: uint32(binPos)})
// 	c.Assert(err, IsNil)

// 	// Test re-sync.
// 	time.Sleep(100 * time.Millisecond)
// 	t.b.c.SetReadDeadline(time.Now().Add(time.Millisecond))
// 	time.Sleep(100 * time.Millisecond)

// 	t.testSync(c, s)
// }

// func (t *testParserSuite) TestMysqlPositionSync(c *C) {
// 	t.setupTest(c, mysql.MySQLFlavor)
// 	t.testPositionSync(c)
// }

// func (t *testParserSuite) TestMysqlGTIDSync(c *C) {
// 	t.setupTest(c, mysql.MySQLFlavor)

// 	r, err := t.c.Execute("SELECT @@gtid_mode")
// 	c.Assert(err, IsNil)
// 	modeOn, _ := r.GetString(0, 0)
// 	if modeOn != "ON" {
// 		c.Skip("GTID mode is not ON")
// 	}

// 	r, err = t.c.Execute("SHOW GLOBAL VARIABLES LIKE 'SERVER_UUID'")
// 	c.Assert(err, IsNil)

// 	var masterUuid uuid.UUID
// 	if s, _ := r.GetString(0, 1); len(s) > 0 && s != "NONE" {
// 		masterUuid, err = uuid.FromString(s)
// 		c.Assert(err, IsNil)
// 	}

// 	set, _ := mysql.ParseMysqlGTIDSet(fmt.Sprintf("%s:%d-%d", masterUuid.String(), 1, 2))

// 	s, err := t.b.StartSyncGTID(set)
// 	c.Assert(err, IsNil)

// 	t.testSync(c, s)
// }

// func (t *testParserSuite) TestMariadbPositionSync(c *C) {
// 	t.setupTest(c, mysql.MariaDBFlavor)

// 	t.testPositionSync(c)
// }

// func (t *testParserSuite) TestMariadbGTIDSync(c *C) {
// 	t.setupTest(c, mysql.MariaDBFlavor)

// 	// get current master gtid binlog pos
// 	r, err := t.c.Execute("SELECT @@gtid_binlog_pos")
// 	c.Assert(err, IsNil)

// 	str, _ := r.GetString(0, 0)
// 	set, _ := mysql.ParseMariadbGTIDSet(str)

// 	s, err := t.b.StartSyncGTID(set)
// 	c.Assert(err, IsNil)

// 	t.testSync(c, s)
// }

// func (t *testParserSuite) TestMariadbAnnotateRows(c *C) {
// 	t.setupTest(c, mysql.MariaDBFlavor)
// 	t.b.cfg.DumpCommandFlag = BINLOG_SEND_ANNOTATE_ROWS_EVENT
// 	t.testPositionSync(c)
// }

// func (t *testParserSuite) TestMysqlSemiPositionSync(c *C) {
// 	t.setupTest(c, mysql.MySQLFlavor)

// 	t.b.cfg.SemiSyncEnabled = true

// 	t.testPositionSync(c)
// }

// func (t *testParserSuite) TestMysqlBinlogCodec(c *C) {
// 	t.setupTest(c, mysql.MySQLFlavor)

// 	t.testExecute(c, "RESET MASTER")

// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	defer wg.Wait()

// 	go func() {
// 		defer wg.Done()

// 		t.testSync(c, nil)

// 		t.testExecute(c, "FLUSH LOGS")

// 		t.testSync(c, nil)
// 	}()

// 	binlogDir := "./var"

// 	os.RemoveAll(binlogDir)

// 	err := t.b.StartBackup(binlogDir, mysql.Position{Name: "", Pos: uint32(0)}, 2*time.Second)
// 	c.Assert(err, IsNil)

// 	p := NewBinlogParser()
// 	p.SetVerifyChecksum(true)

// 	f := func(e *BinlogEvent) error {
// 		if *testOutputLogs {
// 			e.Dump(os.Stdout)
// 			os.Stdout.Sync()
// 		}
// 		return nil
// 	}

// 	dir, err := os.Open(binlogDir)
// 	c.Assert(err, IsNil)
// 	defer dir.Close()

// 	files, err := dir.Readdirnames(-1)
// 	c.Assert(err, IsNil)

// 	for _, file := range files {
// 		err = p.ParseFile(path.Join(binlogDir, file), 0, f)
// 		c.Assert(err, IsNil)
// 	}
// }

func (t *testParserSuite) checkBinlog(c *C, sqls ...string) {
	binlogs := t.getBinlog(c)
	c.Assert(len(binlogs), Equals, len(sqls), Commentf("%#v", binlogs))
	for i, line := range binlogs {
		c.Assert(line, Equals, sqls[i], Commentf("%#v", binlogs))
	}
}

func (t *testParserSuite) getBinlog(c *C) []string {
	// 在线方式解析
	resultOnline := t.getBinlogWithConfig(c, &t.config)

	// 判断本地文件是否存在
	if _, err := os.Stat(t.localConfig.StartFile); err == nil {
		// if runtime.GOOS == "linux" {
		// 本地解析
		resultLocal := t.getBinlogWithConfig(c, &t.localConfig)

		c.Assert(len(resultOnline), Equals, len(resultLocal), Commentf("%#v", resultOnline))
		for i, line := range resultOnline {
			c.Assert(line, Equals, resultLocal[i], Commentf("%#v", resultOnline))
		}
	} else {
		log.Warnf("跳过本地文件解析! 本地文件不存在:%s", t.localConfig.StartFile)
	}

	return resultOnline
}

// getBinlogWithConfig 根据配置文件
func (t *testParserSuite) getBinlogWithConfig(c *C, config *bingo2sql.BinlogParserConfig) []string {

	p, err := bingo2sql.NewBinlogParser(config)
	c.Assert(err, IsNil)

	err = p.Parser()
	c.Assert(err, IsNil)

	fileObj, err := os.Open(config.OutputFileStr)
	c.Assert(err, IsNil)

	defer fileObj.Close()
	//一个文件对象本身是实现了io.Reader的 使用bufio.NewReader去初始化一个Reader对象，存在buffer中的，读取一次就会被清空
	reader := bufio.NewReader(fileObj)

	var buf []string

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				c.Assert(err, IsNil)
				return nil
			}
			break
		}

		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "# ") || line == "" {
			continue
		}

		if strings.Contains(line, "# ") {
			line = line[0:strings.Index(line, "# ")]
		}

		buf = append(buf, strings.TrimSpace(line))
	}
	return buf
}

func (t *testParserSuite) reset() {
	t.config = defaultConfig
	t.localConfig = localConfig
	log.SetLevel(log.ErrorLevel)
}

func (t *testParserSuite) TestSync(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		// "SET SESSION binlog_format = 'MIXED'",
		`DROP TABLE IF EXISTS test_replication`,
		`CREATE TABLE test_replication (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			str VARCHAR(256),
			f FLOAT,
			d DOUBLE,
			de DECIMAL(10,2),
			i INT,
			bi BIGINT,
			e enum ("e1", "e2"),
			b BIT(8),
			y YEAR,
			da DATE,
			ts TIMESTAMP,
			dt DATETIME,
			tm TIME,
			t TEXT,
			bb BLOB,
			se SET('a', 'b', 'c'),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`)

	//use row format
	t.testExecute(c,
		`INSERT INTO test_replication (str, f, i, e, b, y, da, ts, dt, tm, de, t, bb, se)
		VALUES ("3", -3.14, 10, "e1", 0b0011, 1985,
		"2012-05-07", "2012-05-07 14:01:01", "2012-05-07 14:01:01",
		"14:01:01", -45363.64, "abc", "12345", "a,b")`)

	t.checkBinlog(c, "INSERT INTO `test`.`test_replication`(`id`,`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES(1,'3',-3.14,NULL,-45363.64,10,NULL,1,3,1985,'2012-05-07','2012-05-07 14:01:01','2012-05-07 14:01:01','14:01:01','abc','12345',3);")

	t.checkBinlog(c, "INSERT INTO `test`.`test_replication`(`id`,`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES(1,'3',-3.14,NULL,-45363.64,10,NULL,1,3,1985,'2012-05-07','2012-05-07 14:01:01','2012-05-07 14:01:01','14:01:01','abc','12345',3);")

	t.SetFlashback(true)
	t.checkBinlog(c, "DELETE FROM `test`.`test_replication` WHERE `id`=1;")

	t.SetFlashback(false)
	t.SetRemovePrimary(true)
	t.checkBinlog(c, "INSERT INTO `test`.`test_replication`(`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES('3',-3.14,NULL,-45363.64,10,NULL,1,3,1985,'2012-05-07','2012-05-07 14:01:01','2012-05-07 14:01:01','14:01:01','abc','12345',3);")

}

func (t *testParserSuite) TestParseDDL(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		`DROP TABLE IF EXISTS test_replication`,
		`CREATE TABLE test_replication (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			str VARCHAR(256),
			f FLOAT,
			d DOUBLE,
			de DECIMAL(10,2),
			i INT,
			bi BIGINT,
			e enum ("e1", "e2"),
			b BIT(8),
			y YEAR,
			da DATE,
			ts TIMESTAMP,
			dt DATETIME,
			tm TIME,
			t TEXT,
			bb BLOB,
			se SET('a', 'b', 'c'),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`)

	//use row format
	t.testExecute(c,
		`INSERT INTO test_replication (str, f, i, e, b, y, da, ts, dt, tm, de, t, bb, se)
		VALUES ("3", -3.14, 10, "e1", 0b0011, 1985,
		"2012-05-07", "2012-05-07 14:01:01", "2012-05-07 14:01:01",
		"14:01:01", -45363.64, "abc", "12345", "a,b")`)

	t.config.ParseDDL = true
	t.localConfig.ParseDDL = true

	t.checkBinlog(c,
		"USE `test`;",
		"DROP TABLE IF EXISTS `test_replication` /* generated by server */;",
		"USE `test`;",
		"CREATE TABLE test_replication (",
		"id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,",
		"str VARCHAR(256),",
		"f FLOAT,", "d DOUBLE,",
		"de DECIMAL(10,2),", "i INT,",
		"bi BIGINT,", "e enum (\"e1\", \"e2\"),",
		"b BIT(8),", "y YEAR,", "da DATE,",
		"ts TIMESTAMP,", "dt DATETIME,",
		"tm TIME,", "t TEXT,", "bb BLOB,",
		"se SET('a', 'b', 'c'),", "PRIMARY KEY (id)",
		") ENGINE=InnoDB DEFAULT CHARSET=utf8;",
		"INSERT INTO `test`.`test_replication`(`id`,`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES(1,'3',-3.14,NULL,-45363.64,10,NULL,1,3,1985,'2012-05-07','2012-05-07 14:01:01','2012-05-07 14:01:01','14:01:01','abc','12345',3);",
	)

	t.SetFlashback(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_replication` WHERE `id`=1;",
	)

}

func (t *testParserSuite) TestStopTime(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		`DROP TABLE IF EXISTS test_replication`,
		`CREATE TABLE test_replication (
			id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
			str VARCHAR(256),
			f FLOAT,
			d DOUBLE,
			de DECIMAL(10,2),
			i INT,
			bi BIGINT,
			e enum ("e1", "e2"),
			b BIT(8),
			y YEAR,
			da DATE,
			ts TIMESTAMP,
			dt DATETIME,
			tm TIME,
			t TEXT,
			bb BLOB,
			se SET('a', 'b', 'c'),
			PRIMARY KEY (id)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8`)

	//use row format
	t.testExecute(c,
		`INSERT INTO test_replication (str, f, i, e, b, y, da, ts, dt, tm, de, t, bb, se)
		VALUES ("3", -3.14, 10, "e1", 0b0011, 1985,
		"2012-05-07", "2012-05-07 14:01:01", "2012-05-07 14:01:01",
		"14:01:01", -45363.64, "abc", "12345", "a,b")`)

	t.config.StartTime = time.Now().Add(-10 * time.Minute).Format("2006-01-02 15:04")
	t.localConfig.StartTime = t.config.StartTime
	t.checkBinlog(c, "INSERT INTO `test`.`test_replication`(`id`,`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES(1,'3',-3.14,NULL,-45363.64,10,NULL,1,3,1985,'2012-05-07','2012-05-07 14:01:01','2012-05-07 14:01:01','14:01:01','abc','12345',3);")

	t.config.StopTime = time.Now().Add(-5 * time.Minute).Format("2006-01-02 15:04")
	t.localConfig.StopTime = t.config.StopTime
	t.SetFlashback(true)
	// 时间限制范围内无数据
	t.checkBinlog(c)

	t.config.StopTime = time.Now().Add(time.Minute).Format("2006-01-02 15:04:05")
	t.localConfig.StopTime = t.config.StopTime
	t.checkBinlog(c, "DELETE FROM `test`.`test_replication` WHERE `id`=1;")

	t.SetFlashback(false)
	t.SetRemovePrimary(true)
	t.checkBinlog(c, "INSERT INTO `test`.`test_replication`(`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES('3',-3.14,NULL,-45363.64,10,NULL,1,3,1985,'2012-05-07','2012-05-07 14:01:01','2012-05-07 14:01:01','14:01:01','abc','12345',3);")

}
func (t *testParserSuite) TestGeometry(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c,
		"RESET MASTER",
		"DROP TABLE IF EXISTS test_geo",
		`CREATE TABLE test_geo (id int auto_increment primary key, g GEOMETRY)`,
	)

	tbls := []string{
		`INSERT INTO test_geo(g) VALUES (POINT(1, 1))`,
		`INSERT INTO test_geo(g) VALUES (LINESTRING(POINT(0,0), POINT(1,1), POINT(2,2)))`,
		`DELETE from test_geo where id>0`,
	}

	t.testExecute(c, tbls...)

	t.SetFlashback(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_geo` WHERE `id`=1;",
		"DELETE FROM `test`.`test_geo` WHERE `id`=2;",
		"INSERT INTO `test`.`test_geo`(`id`,`g`) VALUES(1,'\\0\\0\\0\\0\x01\x01\\0\\0\\0\\0\\0\\0\\0\\0\\0\xf0?\\0\\0\\0\\0\\0\\0\xf0?');",
		"INSERT INTO `test`.`test_geo`(`id`,`g`) VALUES(2,'\\0\\0\\0\\0\x01\x02\\0\\0\\0\x03\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0\xf0?\\0\\0\\0\\0\\0\\0\xf0?\\0\\0\\0\\0\\0\\0\\0@\\0\\0\\0\\0\\0\\0\\0@');",
	)
}

func (t *testParserSuite) TestDatetime(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c,
		"RESET MASTER",
		`SET sql_mode=''`,
		`DROP TABLE IF EXISTS test_parse_time`,
		`CREATE TABLE test_parse_time (
			id int auto_increment primary key,
			a1 DATETIME,
			a2 DATETIME(3),
			a3 DATETIME(6),
			b1 TIMESTAMP,
			b2 TIMESTAMP(3) ,
			b3 TIMESTAMP(6))`,
	)

	t.testExecute(c, `INSERT INTO test_parse_time(a1,a2,a3,b1,b2,b3) VALUES
		("2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456", "2014-09-08 17:51:04.123456",
		"2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456","2014-09-08 17:51:04.123456"),
		("0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000",
		"0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000", "0000-00-00 00:00:00.000000"),
		("2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456", "2014-09-08 17:51:04.000456",
		"2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456","2014-09-08 17:51:04.000456")`,
		`delete from test_parse_time where id > 0`)

	t.SetFlashback(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_parse_time` WHERE `id`=1;",
		"DELETE FROM `test`.`test_parse_time` WHERE `id`=2;",
		"DELETE FROM `test`.`test_parse_time` WHERE `id`=3;",
		"INSERT INTO `test`.`test_parse_time`(`id`,`a1`,`a2`,`a3`,`b1`,`b2`,`b3`) VALUES(1,'2014-09-08 17:51:04','2014-09-08 17:51:04.123','2014-09-08 17:51:04.123456','2014-09-08 17:51:04','2014-09-08 17:51:04.123','2014-09-08 17:51:04.123456');",
		"INSERT INTO `test`.`test_parse_time`(`id`,`a1`,`a2`,`a3`,`b1`,`b2`,`b3`) VALUES(2,'0000-00-00 00:00:00','0000-00-00 00:00:00.000','0000-00-00 00:00:00.000000','0000-00-00 00:00:00','0000-00-00 00:00:00.000','0000-00-00 00:00:00.000000');",
		"INSERT INTO `test`.`test_parse_time`(`id`,`a1`,`a2`,`a3`,`b1`,`b2`,`b3`) VALUES(3,'2014-09-08 17:51:04','2014-09-08 17:51:04.000','2014-09-08 17:51:04.000456','2014-09-08 17:51:04','2014-09-08 17:51:04.000','2014-09-08 17:51:04.000456');",
	)
}

func (t *testParserSuite) TestBinlogRowImageMinimal(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	id := 100
	t.testExecute(c, `RESET MASTER;`,
		"SET SESSION binlog_row_image = 'MINIMAL'",
		fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb, de) VALUES (%d, "4", -3.14, 100, "abc", -45635.64)`, id),
		fmt.Sprintf(`UPDATE test_replication SET f = -12.14, de = 555.34 WHERE id = %d`, id),
		fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))

	t.SetSQLType("update")

	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `id`=NULL, `str`=NULL, `f`=-12.14, `d`=NULL, `de`=555.34, `i`=NULL, `bi`=NULL, `e`=NULL, `b`=NULL, `y`=NULL, `da`=NULL, `ts`=NULL, `dt`=NULL, `tm`=NULL, `t`=NULL, `bb`=NULL, `se`=NULL WHERE `id`=100;")

	t.SetFlashback(true)
	t.SetMinimalUpdate(true)
	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `id`=100, `f`=NULL, `de`=NULL WHERE `id` IS NULL;",
	)

	t.SetFlashback(false)
	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `id`=NULL, `f`=-12.14, `de`=555.34 WHERE `id`=100;",
	)

	t.testExecute(c, "SET SESSION binlog_row_image = 'FULL'")

}

func (t *testParserSuite) TestMinimalUpdate(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	id := 100
	t.testExecute(c, `RESET MASTER;`,
		fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb, de) VALUES (%d, "4", -3.14, 100, "abc", -45635.64)`, id),
		fmt.Sprintf(`UPDATE test_replication SET f = -12.14, de = 555.34 WHERE id = %d`, id),
		fmt.Sprintf(`UPDATE test_replication SET str=null WHERE id = %d`, id),
		fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))

	t.SetSQLType("update")

	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `id`=100, `str`='4', `f`=-12.14, `d`=NULL, `de`=555.34, `i`=100, `bi`=NULL, `e`=NULL, `b`=NULL, `y`=NULL, `da`=NULL, `ts`=NULL, `dt`=NULL, `tm`=NULL, `t`=NULL, `bb`='abc', `se`=NULL WHERE `id`=100;",
		"UPDATE `test`.`test_replication` SET `id`=100, `str`=NULL, `f`=-12.14, `d`=NULL, `de`=555.34, `i`=100, `bi`=NULL, `e`=NULL, `b`=NULL, `y`=NULL, `da`=NULL, `ts`=NULL, `dt`=NULL, `tm`=NULL, `t`=NULL, `bb`='abc', `se`=NULL WHERE `id`=100;")

	t.SetFlashback(true)
	t.SetMinimalUpdate(true)
	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `f`=-3.14, `de`=-45635.64 WHERE `id`=100;",
		"UPDATE `test`.`test_replication` SET `str`='4' WHERE `id`=100;",
	)

	t.SetFlashback(false)
	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `f`=-12.14, `de`=555.34 WHERE `id`=100;",
		"UPDATE `test`.`test_replication` SET `str`=NULL WHERE `id`=100;",
	)

}

func (t *testParserSuite) TestTextMax(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	id := 100

	// 65536 = 64k
	value := strings.Repeat("a", 1024*1024*10)

	t.testExecute(c, `RESET MASTER;`,
		`DROP TABLE IF EXISTS test_long_text`,
		`CREATE TABLE test_long_text (
				id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
				c1 longtext,
				PRIMARY KEY (id)
				) ENGINE=InnoDB`,
		fmt.Sprintf(`INSERT INTO test_long_text (id, c1) VALUES (%d, "")`, id),
		fmt.Sprintf(`UPDATE test_long_text SET c1 = '%s' WHERE id = %d`, value, id),
		fmt.Sprintf(`DELETE FROM test_long_text WHERE id = %d`, id))

	t.checkBinlog(c,
		"INSERT INTO `test`.`test_long_text`(`id`,`c1`) VALUES(100,'');",
		fmt.Sprintf("UPDATE `test`.`test_long_text` SET `id`=100, `c1`='%s' WHERE `id`=100;", value),
		"DELETE FROM `test`.`test_long_text` WHERE `id`=100;",
	)

	t.SetFlashback(true)
	t.SetMinimalUpdate(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_long_text` WHERE `id`=100;",
		"UPDATE `test`.`test_long_text` SET `c1`='' WHERE `id`=100;",
		fmt.Sprintf("INSERT INTO `test`.`test_long_text`(`id`,`c1`) VALUES(100,'%s');", value),
	)

}

func (t *testParserSuite) TestUpdate2Null(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	id := 100
	t.testExecute(c, `RESET MASTER;`,
		fmt.Sprintf(`INSERT INTO test_replication (id,str, f, i, e, b, y, da, ts, dt, tm, de, t, bb, se)
		VALUES (%d,"3", -3.14, 10, "e1", 0b0011, 1985,
		"2012-05-07", "2012-05-07 14:01:01", "2012-05-07 14:01:01",
		"14:01:01", -45363.64, "abc", "12345", "a,b")`, id),
		fmt.Sprintf(`UPDATE test_replication SET  str = null,f = null,d = null,de = null,i = null,bi = null,e = null,b = null,y = null,da = null,ts = null,dt = null,tm = null,t = null,bb = null WHERE id = %d`, id),
		fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))

	t.SetSQLType("update")

	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `id`=100, `str`=NULL, `f`=NULL, `d`=NULL, `de`=NULL, `i`=NULL, `bi`=NULL, `e`=NULL, `b`=NULL, `y`=NULL, `da`=NULL, `ts`=NULL, `dt`=NULL, `tm`=NULL, `t`=NULL, `bb`=NULL, `se`=3 WHERE `id`=100;")

	t.SetFlashback(true)
	t.SetMinimalUpdate(true)
	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `str`='3', `f`=-3.14, `de`=-45363.64, `i`=10, `e`=1, `b`=3, `y`=1985, `da`='2012-05-07', `ts`='2012-05-07 14:01:01', `dt`='2012-05-07 14:01:01', `tm`='14:01:01', `t`='abc', `bb`='12345' WHERE `id`=100;",
	)

	t.SetFlashback(false)
	t.checkBinlog(c,
		"UPDATE `test`.`test_replication` SET `str`=NULL, `f`=NULL, `de`=NULL, `i`=NULL, `e`=NULL, `b`=NULL, `y`=NULL, `da`=NULL, `ts`=NULL, `dt`=NULL, `tm`=NULL, `t`=NULL, `bb`=NULL WHERE `id`=100;",
	)

}

func (t *testParserSuite) TestRemovePrimary(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	id := 100
	t.testExecute(c, `RESET MASTER;`,
		fmt.Sprintf(`INSERT INTO test_replication (id, str, f, i, bb, de) VALUES (%d, "4", -3.14, 100, "abc", -45635.64)`, id),
		fmt.Sprintf(`DELETE FROM test_replication WHERE id = %d`, id))

	t.checkBinlog(c, "INSERT INTO `test`.`test_replication`(`id`,`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES(100,'4',-3.14,NULL,-45635.64,100,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'abc',NULL);",
		"DELETE FROM `test`.`test_replication` WHERE `id`=100;")

	t.SetFlashback(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_replication` WHERE `id`=100;",
		"INSERT INTO `test`.`test_replication`(`id`,`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES(100,'4',-3.14,NULL,-45635.64,100,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'abc',NULL);",
	)

	t.SetFlashback(false)
	t.SetRemovePrimary(true)
	t.checkBinlog(c,
		"INSERT INTO `test`.`test_replication`(`str`,`f`,`d`,`de`,`i`,`bi`,`e`,`b`,`y`,`da`,`ts`,`dt`,`tm`,`t`,`bb`,`se`) VALUES('4',-3.14,NULL,-45635.64,100,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'abc',NULL);",
		"DELETE FROM `test`.`test_replication` WHERE `id`=100;")

}

// TestThreadID 测试指定线程号,操作后断开重连
func (t *testParserSuite) TestThreadID(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		`DROP TABLE IF EXISTS test_simple`,
		`DROP TABLE IF EXISTS test_simple`,
		`CREATE TABLE test_simple (
				id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
				c1 varchar(100),
				c2 int,
				PRIMARY KEY (id)
				) ENGINE=InnoDB`)

	t.testExecute(c,
		`INSERT INTO test_simple (c1, c2) VALUES ('test',1)`,
		`DELETE FROM test_simple WHERE id > 0`)

	threadID := t.getThreadID(c)

	if t.c != nil {
		t.c.Close()
		t.c = nil
	}

	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c,
		`INSERT INTO test_simple (c1, c2) VALUES ('test2',2)`,
		`DELETE FROM test_simple WHERE id > 0`)

	// 未限制线程号时返回所有数据
	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'test',1);",
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;",
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(2,'test2',2);",
		"DELETE FROM `test`.`test_simple` WHERE `id`=2;")

	t.config.ThreadID = threadID
	t.localConfig.ThreadID = t.config.ThreadID
	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'test',1);",
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;")

	t.SetFlashback(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;",
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'test',1);",
	)
}

// TestInsert 测试insert
func (t *testParserSuite) TestInsert(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		`DROP TABLE IF EXISTS test_simple`,
		`CREATE TABLE test_simple (
				id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
				c1 varchar(100),
				c2 int,
				PRIMARY KEY (id)
				) ENGINE=InnoDB`)

	t.testExecute(c,
		`INSERT INTO test_simple (c1, c2) VALUES ('a1',1),('a2',2),('a3',3),('a4',4),('a5',5)`,
		`DELETE FROM test_simple WHERE id > 0`)

	t.config.MinimalInsert = false
	t.localConfig.MinimalInsert = false

	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'a1',1);",
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(2,'a2',2);",
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(3,'a3',3);",
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(4,'a4',4);",
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(5,'a5',5);",
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=2;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=3;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=4;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=5;")

	t.config.MinimalInsert = true
	t.localConfig.MinimalInsert = true
	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'a1',1),(2,'a2',2),(3,'a3',3),(4,'a4',4),(5,'a5',5);",
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=2;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=3;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=4;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=5;")

	t.SetFlashback(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=2;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=3;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=4;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=5;",
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'a1',1),(2,'a2',2),(3,'a3',3),(4,'a4',4),(5,'a5',5);",
	)
}

// TestThreadID 测试指定线程号,操作后断开重连
func (t *testParserSuite) TestGTID(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		`DROP TABLE IF EXISTS test_simple`,
		`CREATE TABLE test_simple (
				id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
				c1 varchar(100),
				c2 int,
				PRIMARY KEY (id)
				) ENGINE=InnoDB`)

	t.testExecute(c,
		`INSERT INTO test_simple (c1, c2) VALUES ('test',1)`,
		`DELETE FROM test_simple WHERE id > 0`)

	t.testExecute(c,
		`INSERT INTO test_simple (c1, c2) VALUES ('test2',2)`,
		`DELETE FROM test_simple WHERE id > 0`)

	uuid := t.getServerUUID(c)

	// ---- 错误GTID ----

	t.SetIncludeGtids("123")
	_, err := bingo2sql.NewBinlogParser(&t.config)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "错误GTID格式!正确格式为uuid:编号[-编号],多个时以逗号分隔")

	t.SetIncludeGtids(uuid)
	_, err = bingo2sql.NewBinlogParser(&t.localConfig)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "错误GTID格式!正确格式为uuid:编号[-编号],多个时以逗号分隔")

	t.SetIncludeGtids(uuid + ":abc")
	_, err = bingo2sql.NewBinlogParser(&t.localConfig)
	c.Assert(err, NotNil)
	c.Assert(err.Error(), Equals, "GTID解析失败!(strconv.ParseInt: parsing \"abc\": invalid syntax)")

	// ------ end ------

	t.SetIncludeGtids(uuid + ":3")

	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'test',1);")

	t.SetIncludeGtids(uuid + ":3-4")

	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'test',1);",
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;")

	t.SetIncludeGtids(uuid + ":5")
	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(2,'test2',2);")

	t.SetIncludeGtids(uuid + ":3-4," + uuid + ":6-7")

	t.checkBinlog(c,
		"INSERT INTO `test`.`test_simple`(`id`,`c1`,`c2`) VALUES(1,'test',1);",
		"DELETE FROM `test`.`test_simple` WHERE `id`=1;",
		"DELETE FROM `test`.`test_simple` WHERE `id`=2;",
	)

}

func (t *testParserSuite) TestJson(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		`DROP TABLE IF EXISTS test_json`,
		`CREATE TABLE test_json (
				id BIGINT(64) UNSIGNED  NOT NULL AUTO_INCREMENT,
				c1 JSON,
				c2 DECIMAL(10, 0),
				PRIMARY KEY (id)
				) ENGINE=InnoDB`)

	t.testExecute(c,
		`INSERT INTO test_json (c2) VALUES (1)`,
		`INSERT INTO test_json (c1, c2) VALUES ('{"key1": "value1", "key2": "value2"}', 1)`,
		`update test_json set c1 = '{"key1": "value123"}',c2=2000 where id =2`,
		`delete from test_json where id > 0`)

	t.SetFlashback(true)
	t.checkBinlog(c,
		"DELETE FROM `test`.`test_json` WHERE `id`=1;",
		"DELETE FROM `test`.`test_json` WHERE `id`=2;",
		"UPDATE `test`.`test_json` SET `id`=2, `c1`='{\\\"key1\\\":\\\"value1\\\",\\\"key2\\\":\\\"value2\\\"}', `c2`=1 WHERE `id`=2;",
		"INSERT INTO `test`.`test_json`(`id`,`c1`,`c2`) VALUES(1,NULL,1);",
		"INSERT INTO `test`.`test_json`(`id`,`c1`,`c2`) VALUES(2,'{\\\"key1\\\":\\\"value123\\\"}',2000);")
}

func (t *testParserSuite) TestJsonV2(c *C) {
	t.setupTest(c, mysql.MySQLFlavor)

	t.testExecute(c, `RESET MASTER;`,
		"DROP TABLE IF EXISTS test_json_v2",
		`CREATE TABLE test_json_v2 (
				id INT,
				c JSON,
				PRIMARY KEY (id)
				) ENGINE=InnoDB`)

	tbls := []string{
		`INSERT INTO test_json_v2 VALUES (0, NULL)`,
		`INSERT INTO test_json_v2 VALUES (1, '{\"a\": 2}')`,
		`INSERT INTO test_json_v2 VALUES (2, '[1,2]')`,
		`INSERT INTO test_json_v2 VALUES (3, '{\"a\":\"b\", \"c\":\"d\",\"ab\":\"abc\", \"bc\": [\"x\", \"y\"]}')`,
		`INSERT INTO test_json_v2 VALUES (4, '[\"here\", [\"I\", \"am\"], \"!!!\"]')`,
		`INSERT INTO test_json_v2 VALUES (5, '\"scalar string\"')`,
		`INSERT INTO test_json_v2 VALUES (6, 'true')`,
		`INSERT INTO test_json_v2 VALUES (7, 'false')`,
		`INSERT INTO test_json_v2 VALUES (8, 'null')`,
		`INSERT INTO test_json_v2 VALUES (9, '-1')`,
		`INSERT INTO test_json_v2 VALUES (10, CAST(CAST(1 AS UNSIGNED) AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (11, '32767')`,
		`INSERT INTO test_json_v2 VALUES (12, '32768')`,
		`INSERT INTO test_json_v2 VALUES (13, '-32768')`,
		`INSERT INTO test_json_v2 VALUES (14, '-32769')`,
		`INSERT INTO test_json_v2 VALUES (15, '2147483647')`,
		`INSERT INTO test_json_v2 VALUES (16, '2147483648')`,
		`INSERT INTO test_json_v2 VALUES (17, '-2147483648')`,
		`INSERT INTO test_json_v2 VALUES (18, '-2147483649')`,
		`INSERT INTO test_json_v2 VALUES (19, '18446744073709551615')`,
		`INSERT INTO test_json_v2 VALUES (20, '18446744073709551616')`,
		`INSERT INTO test_json_v2 VALUES (21, '3.14')`,
		`INSERT INTO test_json_v2 VALUES (22, '{}')`,
		`INSERT INTO test_json_v2 VALUES (23, '[]')`,
		`INSERT INTO test_json_v2 VALUES (24, CAST(CAST('2015-01-15 23:24:25' AS DATETIME) AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (25, CAST(CAST('23:24:25' AS TIME) AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (125, CAST(CAST('23:24:25.12' AS TIME(3)) AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (225, CAST(CAST('23:24:25.0237' AS TIME(3)) AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (26, CAST(CAST('2015-01-15' AS DATE) AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (27, CAST(TIMESTAMP'2015-01-15 23:24:25' AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (127, CAST(TIMESTAMP'2015-01-15 23:24:25.12' AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (227, CAST(TIMESTAMP'2015-01-15 23:24:25.0237' AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (327, CAST(UNIX_TIMESTAMP('2015-01-15 23:24:25') AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (28, CAST(ST_GeomFromText('POINT(1 1)') AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (29, CAST('[]' AS CHAR CHARACTER SET 'ascii'))`,
		// TODO: 30 and 31 are BIT type from JSON_TYPE, may support later.
		`INSERT INTO test_json_v2 VALUES (30, CAST(x'cafe' AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (31, CAST(x'cafebabe' AS JSON))`,
		`INSERT INTO test_json_v2 VALUES (100, CONCAT('{\"', REPEAT('a', 2 * 100 - 1), '\":123}'))`,
	}

	t.testExecute(c, tbls...)

	t.checkBinlog(c,
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(0,NULL);",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(1,'{\\\"a\\\":2}');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(2,'[1,2]');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(3,'{\\\"a\\\":\\\"b\\\",\\\"ab\\\":\\\"abc\\\",\\\"bc\\\":[\\\"x\\\",\\\"y\\\"],\\\"c\\\":\\\"d\\\"}');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(4,'[\\\"here\\\",[\\\"I\\\",\\\"am\\\"],\\\"!!!\\\"]');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(5,'\\\"scalar string\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(6,'true');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(7,'false');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(8,'null');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(9,'-1');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(10,'1');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(11,'32767');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(12,'32768');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(13,'-32768');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(14,'-32769');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(15,'2147483647');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(16,'2147483648');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(17,'-2147483648');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(18,'-2147483649');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(19,'18446744073709551615');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(20,'18446744073709552000');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(21,'3.14');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(22,'{}');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(23,'[]');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(24,'\\\"2015-01-15 23:24:25.000000\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(25,'\\\"23:24:25.000000\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(125,'\\\"23:24:25.120000\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(225,'\\\"23:24:25.024000\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(26,'\\\"2015-01-15 00:00:00.000000\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(27,'\\\"2015-01-15 23:24:25.000000\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(127,'\\\"2015-01-15 23:24:25.120000\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(227,'\\\"2015-01-15 23:24:25.023700\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(327,'1421335465');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(28,'{\\\"coordinates\\\":[1,1],\\\"type\\\":\\\"Point\\\"}');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(29,'[]');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(30,'\\\"\\\\ufffd\\\\ufffd\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(31,'\\\"\\\\ufffd\\\\ufffd\\\\ufffd\\\\ufffd\\\"');",
		"INSERT INTO `test`.`test_json_v2`(`id`,`c`) VALUES(100,'{\\\"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\\\":123}');")

	t.testExecute(c, "delete from test_json_v2 where id >0")
}

func (t *testParserSuite) initTableSchema(tableName ...string) {

	var tables []string

	if len(tableName) > 0 {
		for _, name := range tableName {
			if v, ok := allTables[name]; ok {
				tables = append(tables, v)
			}
		}
	} else {
		for _, value := range allTables {
			tables = append(tables, value)
		}
	}

	err := ioutil.WriteFile(tableSchemaFile, []byte(strings.Join(tables, ";\n")), 0666)
	if err != nil {
		log.Fatal(err)
	}
}

// createTable 初始化时创建所有表
func (t *testParserSuite) createTables(c *C) {
	var tables []string
	for _, value := range allTables {
		tables = append(tables, value)
	}
	t.testExecute(c, tables...)
}
