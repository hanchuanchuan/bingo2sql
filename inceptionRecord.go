// go-mysqlbinlog: a simple binlog tool to sync remote MySQL binlog.
// go-mysqlbinlog supports semi-sync mode like facebook mysqlbinlog.
// see http://yoshinorimatsunobu.blogspot.com/2014/04/semi-synchronous-replication-at-facebook.html
package bingo2sql

// time go run mainRemote.go -start-time="2018-09-17 00:00:00" -stop-time="2018-09-25 00:00:00" -o=1.sql
import (
    "fmt"
    "github.com/jinzhu/gorm"
    _ "github.com/jinzhu/gorm/dialects/mysql"
    "github.com/juju/errors"
    "github.com/rs/zerolog/log"
)

type SqlIncRecord struct {
    gorm.Model
    // ID           uint   `gorm:"Column:id"`
    // ID           uint64 `gorm:"Column:id"`
    BackupDBName string `gorm:"Column:backup_dbname"`
    OPID         string `gorm:"Column:sequence"`
}

type BackupInfo struct {
    gorm.Model
    OPID          string `gorm:"Column:opid_time"`
    StartFile     string `gorm:"Column:start_binlog_file"`
    StartPosition int    `gorm:"Column:start_binlog_pos"`
    StopFile      string `gorm:"Column:end_binlog_file"`
    StopPosition  int    `gorm:"Column:end_binlog_pos"`
    Host          string `gorm:"Column:host"`
    Port          uint16 `gorm:"Column:port"`
    DB            string `gorm:"Column:dbname"`
    Table         string `gorm:"Column:tablename"`
    Type          string `gorm:"Column:type"`
}

type BackupInfos []*BackupInfo

type Iterator struct {
    data        BackupInfos
    index       int
    r           *incRecords
    ExistsTable map[string]bool
}

type incRecords struct {
    work_id string
    db_id   int

    fetchEnd bool
    lastId   uint

    // cmdb数据库
    localdb *gorm.DB

    // inception备份数据库
    backupdb *gorm.DB
}

func NewIterator(r *incRecords) *Iterator {

    r.lastId = 0
    r.fetchEnd = false

    return &Iterator{
        index:       0,
        r:           r,
        ExistsTable: make(map[string]bool),
    }
}

func (i *BackupInfos) Iterator(r *incRecords) *Iterator {
    return &Iterator{
        data:        *i,
        index:       0,
        r:           r,
        ExistsTable: make(map[string]bool),
    }
}

func (i *Iterator) First() *BackupInfo {

    i.fetchNext()

    if len(i.data) > 0 {
        return i.data[0]
    } else {
        return nil
    }
}

func (i *Iterator) HasNext() bool {
    if i.index < len(i.data) {
        return true
    }

    if i.r.fetchEnd {
        return false
    } else {
        return i.fetchNext()
    }
}

func (i *Iterator) Next() *BackupInfo {

    v := i.data[i.index]
    i.index++
    return v
}

func (i *Iterator) fetchNext() bool {

    sql := `select id, backup_dbname, replace(sequence,'''','')  as sequence
        from sql_inc_record
        where stage='EXECUTED' and backup_dbname is not null and backup_dbname <>'None'
            and work_id = ? `

    if i.r.db_id > 0 {
        sql = fmt.Sprintf("%s and db_id = ? ", sql)
    } else {
        sql = fmt.Sprintf("%s and db_id is null ", sql)
    }

    sql = fmt.Sprintf("%s and id > ? ", sql)

    sql = fmt.Sprintf("%s order by `file`,order_id limit 500", sql)

    var records []SqlIncRecord

    // i.r.localdb.SetLogger(log.New(os.Stdout, "\r\n", 0))

    // log.Info().Uint64("lastid", i.r.lastId).Int("len", len(records)).Msg("上一次结束id")

    if i.r.db_id > 0 {
        i.r.localdb.Raw(sql, i.r.work_id, i.r.db_id, i.r.lastId).Scan(&records)
    } else {
        i.r.localdb.Raw(sql, i.r.work_id, i.r.lastId).Scan(&records)
    }

    i.r.localdb.Raw(sql, i.r.work_id, i.r.lastId).Scan(&records)

    log.Info().Uint("last", i.r.lastId).Int("len", len(records)).Msg("上一次结束id")

    // host, port := i.getDBAddress(i.r.work_id, i.r.db_id)

    if len(records) < 500 {
        i.r.fetchEnd = true
    }

    if len(records) > 0 {
        i.r.lastId = records[len(records)-1].ID
        if i.r.lastId == 0 {
            fmt.Println(sql, i.r.work_id, i.r.db_id, i.r.lastId)
            fmt.Println(records[len(records)-1].ID, records[len(records)-1])
            panic(errors.New("死循环!"))
        }
        i.data = i.getInceptionBackupRecords(records)
        i.index = 0
        return true
    } else {
        return false
    }

}

func (i *Iterator) getDBAddress(work_id string, dbId int) (host string, port int) {
    log.Info().Msg("getDbaddress")

    var sql string
    if dbId > 0 {
        sql = `select case when host.vip is null or host.vip = "" then host.ip else host.vip end as ip,
        instance.port from host inner join instance on host.id = instance.host_id
        inner join db on instance.id = db.ins_id
        where db.id = ?;`

        rows, err := i.r.localdb.Raw(sql, dbId).Rows()
        if rows != nil {
            defer rows.Close()
        }

        if err != nil {
            fmt.Println("getDBAddress", err)
        }

        for rows.Next() {
            rows.Scan(&host, &port)
        }
    } else {
        sql = `select case when host.vip is null or host.vip = "" then host.ip else host.vip end as ip,
        instance.port from host inner join sql_inc_order on host.id = sql_inc_order.host_id
        inner join instance on host.id = instance.host_id and instance.id =  sql_inc_order.instance_id
        where sql_inc_order.work_id = ?;`

        rows, err := i.r.localdb.Raw(sql, work_id).Rows()
        if rows != nil {
            defer rows.Close()
        }

        if err != nil {
            fmt.Println("getDBAddress", err)
        }

        for rows.Next() {
            rows.Scan(&host, &port)
        }
    }

    log.Info().Msg(fmt.Sprintf("%s,%d,%s,%d", work_id, dbId, host, port))
    return
}

func (i *Iterator) getInceptionBackupRecords(records []SqlIncRecord) BackupInfos {

    db := i.r.backupdb
    var (
        opids  []string
        result BackupInfos
        tmp    []*BackupInfo

        // 限制每次查询的最大行数500 ! 不再限制,由外围限制
        // maxLentgh int = 500
        // length    int = 0
    )
    lastDBName := ""

    sql := `SELECT * FROM %s.$_$Inception_backup_information$_$
        WHERE opid_time IN (?) and start_binlog_pos > 0 ORDER BY opid_time `

    for _, r := range records {
        // fmt.Printf("%#v\n", r)

        if lastDBName == "" || lastDBName == r.BackupDBName {
            lastDBName = r.BackupDBName
            opids = append(opids, r.OPID)

        } else {

            db.Raw(fmt.Sprintf(sql, lastDBName), opids).Scan(&tmp)

            // for _, row := range tmp {
            // 当实例的端口和数据库端口不一致时,表明是中间件数据库,此时替换为实际IP和PORT
            // fmt.Println(row.Host, row.Port, host, port)
            // if int(row.Port) != port {
            //     row.Host = host
            //     row.Port = uint16(port)
            // }
            // }

            result = append(result, tmp...)

            i.findNeedClearTable(tmp, lastDBName)
            tmp = nil
            opids = nil
            opids = append(opids, r.OPID)
            lastDBName = r.BackupDBName
        }
    }

    db.Raw(fmt.Sprintf(sql, lastDBName), opids).Scan(&tmp)
    result = append(result, tmp...)

    i.findNeedClearTable(tmp, lastDBName)
    tmp = nil
    opids = nil

    return result
}

// 查找并清除已存在的数据,避免回滚语句重复
func (i *Iterator) findNeedClearTable(rows []*BackupInfo, dbname string) {

    lastTable := ""
    // fmt.Println("findNeedClearTable", len(rows))
    var opids []string
    for _, row := range rows {
        if lastTable == "" {
            lastTable = row.Table
        }

        if lastTable != row.Table {
            i.clearTableData(dbname, lastTable, opids)
            lastTable = row.Table
            opids = nil
        }
        opids = append(opids, row.OPID)
    }

    if len(opids) > 0 {
        i.clearTableData(dbname, lastTable, opids)
    }
}

// 清除已生成的回滚语句
func (i *Iterator) clearTableData(dbname string, table string, opids []string) {
    db := i.r.backupdb

    fullName := fmt.Sprintf("%s.dba_%s", dbname, table)
    if db.Debug().HasTable(fullName) {
        i.ExistsTable[fullName] = true

        deleteSql := "DELETE FROM `%s`.`dba_%s` where opid_time IN (?) "

        db.Exec(fmt.Sprintf(deleteSql, dbname, table), opids)

    } else {
        i.ExistsTable[fullName] = false
    }
}
