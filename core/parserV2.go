package core

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/shopspring/decimal"
	log "github.com/sirupsen/logrus"
)

// generateInsertSQL 生成insert语句
func (p *MyBinlogParser) generateInsertSQL_2(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {

	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s.%s缺少列!当前列数:%d,binlog的列数%d",
			e.Table.Schema, e.Table.Table, len(t.Columns), e.ColumnCount)
	}

	var columnNames []string
	c := "`%s`"

	buf := &bytes.Buffer{}
	buf.WriteString("INSERT ")
	// if s.ignore {
	// 	_, _ = buf.WriteString("IGNORE ")
	// }
	buf.WriteString("INTO ")
	buf.WriteString("`")
	buf.Write(e.Table.Schema)
	buf.WriteString("`.`")
	buf.Write(e.Table.Table)
	buf.WriteString("`")
	buf.WriteString("(")
	strings.Join(columnNames, ",")

	for i, col := range t.Columns {
		if i < int(e.ColumnCount) {
			//  有主键且设置去除主键时 作特殊处理
			if t.hasPrimary && p.cfg.RemovePrimary {
				if _, ok := t.primarys[i]; !ok {
					columnNames = append(columnNames, fmt.Sprintf(c, col.ColumnName))
				}
			} else {
				columnNames = append(columnNames, fmt.Sprintf(c, col.ColumnName))
			}
		}
	}

	buf.WriteString(strings.Join(columnNames, ","))
	buf.WriteString(") VALUES")

	var insertSQL string
	if !p.Config().MinimalInsert {
		insertSQL = buf.String()
	}
	for rowIndex, rows := range e.Rows {
		if rowIndex > 0 {
			if p.Config().MinimalInsert {
				buf.WriteString(",")
			} else {
				buf.WriteString(insertSQL)
			}
		}

		buf.WriteString("(")

		for colIndex, d := range rows {
			if t.hasPrimary && p.cfg.RemovePrimary {
				if _, ok := t.primarys[colIndex]; ok {
					continue
				}
			}

			if t.Columns[colIndex].IsUnsigned() {
				d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
			}
			v, err := valueSerialize(d)
			if err != nil {
				log.Error(err)
				return err
			}
			if colIndex > 0 {
				buf.WriteByte(',')
			}
			buf.Write(v)
		}

		buf.WriteString(")")

		if !p.cfg.MinimalInsert {
			p.write(buf.Bytes(), binEvent)
			buf = &bytes.Buffer{}
		}
	}

	if p.cfg.MinimalInsert {
		p.write(buf.Bytes(), binEvent)
		buf.Reset()
	}

	return nil
}

// generateDeleteSQL 生成delete语句
func (p *MyBinlogParser) generateDeleteSQL_2(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {

	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s.%s缺少列!当前列数:%d,binlog的列数%d",
			e.Table.Schema, e.Table.Table, len(t.Columns), e.ColumnCount)
	}

	for _, rows := range e.Rows {

		buf := &bytes.Buffer{}
		buf.WriteString("DELETE FROM ")
		buf.WriteString("`")
		buf.Write(e.Table.Schema)
		buf.WriteString("`.`")
		buf.Write(e.Table.Table)
		buf.WriteString("` WHERE ")

		initFirst := false
		for colIndex, d := range rows {
			if t.hasPrimary {
				if _, ok := t.primarys[colIndex]; ok {
					if initFirst {
						buf.WriteString(" AND ")
					}
					initFirst = true
					if t.Columns[colIndex].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
					}
					if d == nil {
						buf.WriteString("`")
						buf.WriteString(t.Columns[colIndex].ColumnName)
						buf.WriteString("` IS NULL")
					} else {
						v, err := valueSerialize(d)
						if err != nil {
							log.Error(err)
						}
						buf.WriteString("`")
						buf.WriteString(t.Columns[colIndex].ColumnName)
						buf.WriteString("`=")
						buf.Write(v)
					}
				}
			} else {
				if initFirst {
					buf.WriteString(" AND ")
				}
				initFirst = true
				if t.Columns[colIndex].IsUnsigned() {
					d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
				}
				if d == nil {
					buf.WriteString("`")
					buf.WriteString(t.Columns[colIndex].ColumnName)
					buf.WriteString("` IS NULL")
				} else {
					v, err := valueSerialize(d)
					if err != nil {
						log.Error(err)
					}
					buf.WriteString("`")
					buf.WriteString(t.Columns[colIndex].ColumnName)
					buf.WriteString("`=")
					buf.Write(v)
				}
			}
		}

		p.write(buf.Bytes(), binEvent)
		// buf = &bytes.Buffer{}
	}

	return nil
}

// generateUpdateSQL 生成update语句
func (p *MyBinlogParser) generateUpdateSQL_2(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {

	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s.%s缺少列!当前列数:%d,binlog的列数%d",
			e.Table.Schema, e.Table.Table, len(t.Columns), e.ColumnCount)
	}

	// 最小化回滚语句, 当开启时,update语句中未变更的值不再记录到回滚语句中
	minimalMode := p.cfg.MinimalUpdate

	// update时, Rows为2的倍数, 双数index为旧值,单数index为新值
	buf := &bytes.Buffer{}
	bufWhere := &bytes.Buffer{}

	for rowIndex, row := range e.Rows {

		if rowIndex%2 == 0 {
			// 旧值
			for colIndex, d := range row {
				if t.hasPrimary {
					if _, ok := t.primarys[colIndex]; ok {
						if bufWhere.Len() > 0 {
							bufWhere.WriteString(" AND ")
						}
						if d == nil {
							bufWhere.WriteString("`")
							bufWhere.WriteString(t.Columns[colIndex].ColumnName)
							bufWhere.WriteString("` IS NULL")
						} else {
							bufWhere.WriteString("`")
							bufWhere.WriteString(t.Columns[colIndex].ColumnName)
							bufWhere.WriteString("`=")

							v, err := valueSerialize(d)
							if err != nil {
								log.Error(err)
							}
							bufWhere.Write(v)
						}
					}
				} else {
					if colIndex > 0 {
						bufWhere.WriteString(" AND ")
					}

					if t.Columns[colIndex].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
					}

					if d == nil {
						bufWhere.WriteString("`")
						bufWhere.WriteString(t.Columns[colIndex].ColumnName)
						bufWhere.WriteString("` IS NULL")
					} else {
						v, err := valueSerialize(d)
						if err != nil {
							log.Error(err)
						}
						bufWhere.WriteString("`")
						bufWhere.WriteString(t.Columns[colIndex].ColumnName)
						bufWhere.WriteString("`=")
						bufWhere.Write(v)
					}
				}
			}
		} else {
			initFirst := false

			buf.WriteString("UPDATE ")
			buf.WriteString("`")
			buf.Write(e.Table.Schema)
			buf.WriteString("`.`")
			buf.Write(e.Table.Table)
			buf.WriteString("` SET ")

			for colIndex, d := range row {
				if minimalMode {
					// 最小化模式下,列如果相等则省略
					if !compareValue(d, e.Rows[rowIndex-1][colIndex]) {
						if initFirst {
							buf.WriteByte(',')
						}
						initFirst = true
						if t.Columns[colIndex].IsUnsigned() {
							d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
						}
						v, err := valueSerialize(d)
						if err != nil {
							log.Error(err)
						}

						buf.WriteString("`")
						buf.WriteString(t.Columns[colIndex].ColumnName)
						buf.WriteString("`=")
						buf.Write(v)
					}
				} else {
					if colIndex > 0 {
						buf.WriteByte(',')
					}
					if t.Columns[colIndex].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
					}
					v, err := valueSerialize(d)
					if err != nil {
						log.Error(err)
					}
					buf.WriteString("`")
					buf.WriteString(t.Columns[colIndex].ColumnName)
					buf.WriteString("`=")
					buf.Write(v)
				}
			}

			buf.WriteString(" WHERE ")
			buf.Write(bufWhere.Bytes())

			p.write(buf.Bytes(), binEvent)

			buf = &bytes.Buffer{}
			bufWhere = &bytes.Buffer{}
		}
	}

	return nil
}

// generateUpdateRollbackSQL 生成update语句
func (p *MyBinlogParser) generateUpdateRollbackSQL_2(t *Table, e *replication.RowsEvent,
	binEvent *replication.BinlogEvent) error {

	if len(t.Columns) < int(e.ColumnCount) {
		return fmt.Errorf("表%s.%s缺少列!当前列数:%d,binlog的列数%d",
			e.Table.Schema, e.Table.Table, len(t.Columns), e.ColumnCount)
	}

	// 最小化回滚语句, 当开启时,update语句中未变更的值不再记录到回滚语句中
	minimalMode := p.cfg.MinimalUpdate

	// update时, Rows为2的倍数, 双数index为旧值,单数index为新值
	buf := &bytes.Buffer{}
	bufWhere := &bytes.Buffer{}

	for rowIndex, rows := range e.Rows {
		// 旧值
		if rowIndex%2 == 0 {

			buf.WriteString("UPDATE ")
			buf.WriteString("`")
			buf.Write(e.Table.Schema)
			buf.WriteString("`.`")
			buf.Write(e.Table.Table)
			buf.WriteString("` SET ")

			initFirst := false
			for colIndex, d := range rows {
				if minimalMode {
					// 最小化模式下,列如果相等则省略
					if !compareValue(d, e.Rows[rowIndex+1][colIndex]) {
						if initFirst {
							buf.WriteString(",")
						}
						initFirst = true
						if t.Columns[colIndex].IsUnsigned() {
							d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
						}
						v, err := valueSerialize(d)
						if err != nil {
							log.Error(err)
						}

						buf.WriteString("`")
						buf.WriteString(t.Columns[colIndex].ColumnName)
						buf.WriteString("`=")
						buf.Write(v)
					}
				} else {
					if colIndex > 0 {
						buf.WriteString(",")
					}
					if t.Columns[colIndex].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
					}
					v, err := valueSerialize(d)
					if err != nil {
						log.Error(err)
					}

					buf.WriteString("`")
					buf.WriteString(t.Columns[colIndex].ColumnName)
					buf.WriteString("`=")
					buf.Write(v)
				}
			}
		} else { // 新值
			for colIndex, d := range rows {
				if t.hasPrimary {
					_, ok := t.primarys[colIndex]
					if ok {
						if t.Columns[colIndex].IsUnsigned() {
							d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
						}
						if bufWhere.Len() > 0 {
							bufWhere.WriteString(" AND ")
						}
						if d == nil {
							bufWhere.WriteString("`")
							bufWhere.WriteString(t.Columns[colIndex].ColumnName)
							bufWhere.WriteString("` IS NULL")
						} else {
							bufWhere.WriteString("`")
							bufWhere.WriteString(t.Columns[colIndex].ColumnName)
							bufWhere.WriteString("`=")

							v, err := valueSerialize(d)
							if err != nil {
								log.Error(err)
							}
							bufWhere.Write(v)
						}
					}
				} else {
					if t.Columns[colIndex].IsUnsigned() {
						d = processValue(d, GetDataTypeBase(t.Columns[colIndex].ColumnType))
					}
					if bufWhere.Len() > 0 {
						bufWhere.WriteString(" AND ")
					}
					if d == nil {
						bufWhere.WriteString("`")
						bufWhere.WriteString(t.Columns[colIndex].ColumnName)
						bufWhere.WriteString("` IS NULL")
					} else {
						bufWhere.WriteString("`")
						bufWhere.WriteString(t.Columns[colIndex].ColumnName)
						bufWhere.WriteString("`=")

						v, err := valueSerialize(d)
						if err != nil {
							log.Error(err)
						}
						bufWhere.Write(v)
					}
				}
			}

			buf.WriteString(" WHERE ")
			buf.Write(bufWhere.Bytes())

			p.write(buf.Bytes(), binEvent)

			buf = &bytes.Buffer{}
			bufWhere = &bytes.Buffer{}
		}

	}

	return nil
}

// valueSerialize 参数转换成数据库类型
func valueSerialize(arg driver.Value) (buf []byte, err error) {

	if arg == nil {
		buf = append(buf, "NULL"...)
		return buf, nil
	}

	switch v := arg.(type) {
	case int8:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int16:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int32:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case int64:
		buf = strconv.AppendInt(buf, v, 10)
	case uint64:
		buf = strconv.AppendUint(buf, uint64(v), 10)
	case int:
		buf = strconv.AppendInt(buf, int64(v), 10)
	case decimal.Decimal:
		buf = append(buf, v.String()...)
	case float32:
		buf = strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
	case float64:
		buf = strconv.AppendFloat(buf, v, 'g', -1, 64)
	case bool:
		if v {
			buf = append(buf, '1')
		} else {
			buf = append(buf, '0')
		}
	case time.Time:
		if v.IsZero() {
			buf = append(buf, "'0000-00-00'"...)
		} else {
			v := v.In(time.UTC)
			v = v.Add(time.Nanosecond * 500) // To round under microsecond
			year := v.Year()
			year100 := year / 100
			year1 := year % 100
			month := v.Month()
			day := v.Day()
			hour := v.Hour()
			minute := v.Minute()
			second := v.Second()
			micro := v.Nanosecond() / 1000

			buf = append(buf, []byte{
				'\'',
				digits10[year100], digits01[year100],
				digits10[year1], digits01[year1],
				'-',
				digits10[month], digits01[month],
				'-',
				digits10[day], digits01[day],
				' ',
				digits10[hour], digits01[hour],
				':',
				digits10[minute], digits01[minute],
				':',
				digits10[second], digits01[second],
			}...)

			if micro != 0 {
				micro10000 := micro / 10000
				micro100 := micro / 100 % 100
				micro1 := micro % 100
				buf = append(buf, []byte{
					'.',
					digits10[micro10000], digits01[micro10000],
					digits10[micro100], digits01[micro100],
					digits10[micro1], digits01[micro1],
				}...)
			}
			buf = append(buf, '\'')
		}
	case string:
		buf = append(buf, '\'')
		buf = escapeBytesBackslash(buf, []byte(v))
		buf = append(buf, '\'')
	case []byte:
		if v == nil {
			buf = append(buf, "NULL"...)
		} else {
			// buf = append(buf, "_binary'"...)
			buf = append(buf, '\'')

			buf = escapeBytesBackslash(buf, v)

			buf = append(buf, '\'')
		}
	default:
		// fmt.Println(v)
		log.Printf("%T", v)
		log.Info("解析错误")
		return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
	}

	// 4 << 20 , 4MB
	if len(buf)+4 > 4<<20 {
		// log.Print("%T", v)
		log.Info("解析错误")
		return nil, errors.New("driver: skip fast-path; continue as if unimplemented")
	}

	return buf, nil
}
