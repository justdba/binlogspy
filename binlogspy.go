package main

import (
	. "binlog-spy/mello"
	"context"
	"fmt"
	"github.com/percona/go-mysql/query"
	"github.com/siddontang/go-mysql/mysql"
	. "github.com/siddontang/go-mysql/replication"
	"strings"
	"time"
)


func main() {
	tableMap := make(map[string]int)
	tableDMLmap := make(map[string]int)
	sqlMap := make(map[string]int)
	longSingleTranMap := make(map[string]int)
	largeSingleTranMap := make(map[string]int)
	DMLTranCountMap := make(map[string]int)
	DMLTypeSizeMap := make(map[string]int)

	var (
		longTotalSQL   []StmtInfo
		largeTotalSQL  []StmtInfo
		longTotalTran  []StmtInfo
		largeTotalTran []StmtInfo

		sqltext      string
		dbname       string
		table        string
		keyDMLType   string
		keyTab       string
		startTime    uint32
		sqlStartTime uint32
		tabList      []string
	)

	// init value
	dmlType := ""
	rotateFlag := 0
	tableMapFlag := 0
	tranSize := 0
	UnixNow := uint32(time.Now().Unix())

	//read binlog
	param := InputInfo()
	syncer := NewBinlogSyncer(param.Cfg)
	streamer, _ := syncer.StartSync(mysql.Position{param.BinlogName, 0})

	for {
		//read single binlog
		ev, _ := streamer.GetEventNoNew(context.Background())

		//quit if no new binlog  or  stop scan active binlog at call-time
		if ev == nil || ev.Header.Timestamp > UnixNow {
			break
		}

		if param.InputStartTime != 0 && param.InputEndTime != 0 {
			// reduce scan from input time-range
			if ev.Header.Timestamp > param.InputEndTime {
				break
			}
			if ev.Header.Timestamp < param.InputStartTime {
				continue
			}
		}

		evt := ev.Header.EventType
		if evt == ROTATE_EVENT && rotateFlag == 0 {
			rotateFlag = 1

		} else if evt == QUERY_EVENT {
			//tran starttime
			startTime = ev.Header.Timestamp

		} else if evt == ROWS_QUERY_EVENT {
			//sql starttime
			sqlStartTime = ev.Header.Timestamp
			sqltext = strings.TrimSpace(string(ev.Event.(*RowsQueryEvent).Query))
			if sqltext[0:2] == "/*" {
				idx := strings.Index(sqltext, "*/") + 2
				sqltext = strings.TrimSpace(sqltext[idx:])
			}

			fingerprint := query.Fingerprint(sqltext)
			if !KeyInMap(fingerprint, sqlMap) {
				sqlMap[fingerprint] = 1
			} else {
				sqlMap[fingerprint] = sqlMap[fingerprint] + 1
			}

			if len(sqltext) > param.MaxSqllen {
				sqltext = sqltext[0:param.MaxSqllen]
			}
			dmlType = strings.ToLower(sqltext[0:6])

		} else if evt == TABLE_MAP_EVENT {
			dbname = string(ev.Event.(*TableMapEvent).Schema)
			table = string(ev.Event.(*TableMapEvent).Table)

			keyTab = fmt.Sprintf("%s.%s", dbname, table)
			if !KeyInMap(keyTab, tableMap) {
				tableMap[keyTab] = 1
			} else {
				tableMap[keyTab] = tableMap[keyTab] + 1
			}

			keyDMLType = fmt.Sprintf("%s.%s.%s", dbname, table, dmlType)
			if !KeyInMap(keyDMLType, tableDMLmap) {
				tableDMLmap[keyDMLType] = 1
			} else {
				tableDMLmap[keyDMLType] = tableDMLmap[keyDMLType] + 1
			}

			//find table in tran, save only one time
			if !KeyInList(keyDMLType, tabList) {
				tabList = append(tabList, keyDMLType)
			}

			//a big tran auto split to multi xxx_row_event, must save only one time
			tableMapFlag = 1

		} else if evt == WRITE_ROWS_EVENTv2 || evt == UPDATE_ROWS_EVENTv2 || evt == DELETE_ROWS_EVENTv2 {
			//sql intervals
			sqlIntervals := int(ev.Header.Timestamp - sqlStartTime)
			if tableMapFlag == 1 {
				longTotalSQL = append(longTotalSQL, StmtInfo{sqltext, sqlIntervals, sqlStartTime})
				//single tran intervals, only save first one
				longSingleTranMap[sqltext] = sqlIntervals
			}

			//sql event size
			esize := int(ev.Header.EventSize)
			if tableMapFlag == 1 {
				largeTotalSQL = append(largeTotalSQL, StmtInfo{sqltext, esize, sqlStartTime})
				//single tran event size, only save first one
				largeSingleTranMap[sqltext] = esize
			}

			//a big tran auto split to multi xxx_row_event, must save only one time
			tableMapFlag = 0

			// sum event size for each keyDMLType
			if !KeyInMap(keyDMLType, DMLTypeSizeMap) {
				DMLTypeSizeMap[keyDMLType] = esize
			} else {
				DMLTypeSizeMap[keyDMLType] = DMLTypeSizeMap[keyDMLType] + esize
			}

			tranSize += esize

		} else if evt == XID_EVENT {
			//tran Intervals
			tranIntervals := int(ev.Header.Timestamp - startTime)
			longTotalTran = append(longTotalTran, StmtInfo{longSingleTranMap, tranIntervals, startTime})
			//clear longSingleTranMap
			longSingleTranMap = make(map[string]int)

			//tran size
			largeTotalTran = append(largeTotalTran, StmtInfo{largeSingleTranMap, tranSize, startTime})
			//clear largeSingleTranMap
			largeSingleTranMap = make(map[string]int)
			tranSize = 0

			//DML Tran Times
			for _, v := range tabList {
				if !KeyInMap(v, DMLTranCountMap) {
					DMLTranCountMap[v] = 1
				} else {
					DMLTranCountMap[v] = DMLTranCountMap[v] + 1
				}
			}
			tabList = nil

		} else if evt == ROTATE_EVENT && rotateFlag == 1 {
			break
		}
	}
	syncer.Close()

	Output(param, tableMap, "Table Change Times")
	OutputPlus(param, tableDMLmap, "Table DML Times", DMLTranCountMap, DMLTypeSizeMap)
	Output(param, sqlMap, "SQL Times")
	OutputStmtInfo(param, longTotalSQL, "Long SQL (secs)")
	OutputStmtInfo(param, largeTotalSQL, "Large SQL (bytes)")
	OutputStmtInfo(param, longTotalTran, "Long Trans (secs)")
	OutputStmtInfo(param, largeTotalTran, "large Trans (bytes)")

}

