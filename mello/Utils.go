package mello

import (
	"flag"
	"fmt"
	"github.com/blastrain/vitess-sqlparser/sqlparser"
	. "github.com/siddontang/go-mysql/replication"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"
)

/*
implicit assignment of unexported field '' in  literal
variables used for others package, first char must be upper
*/

func KeyInMap(e string, tableMap map[string]int) bool {
	for k := range tableMap {
		if k == e {
			return true
		}
	}
	return false
}

func KeyInList(e string, tableList []string) bool {
	for _, v := range tableList {
		if v == e {
			return true
		}
	}
	return false
}

func rankByCount(myMap map[string]int) PairList {
	pl := make(PairList, len(myMap))
	i := 0
	for k, v := range myMap {
		pl[i] = Pair{k, v}
		i++
	}
	//sort.Sort(pl)
	sort.Sort(sort.Reverse(pl))
	return pl
}

func rankByCountStmtInfo(myStmt []StmtInfo) PairListStmt {
	pl := make(PairListStmt, len(myStmt))
	i := 0
	for _, v := range myStmt {
		pl[i] = PairStmt{v.Key, v.Value, v.Dt}
		i++
	}
	//sort.Sort(pl)
	sort.Sort(sort.Reverse(pl))
	return pl
}

type PairStmt struct {
	Key   interface{}
	Value int
	Dt    uint32
}

type PairListStmt []PairStmt

func (p PairListStmt) Len() int           { return len(p) }
func (p PairListStmt) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairListStmt) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type Pair struct {
	Key   interface{}
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value < p[j].Value }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Output(param *BinlogInfo, myMap map[string]int, title string) {
	if len(myMap) == 0 {
		return
	}
	fmt.Printf("\n==================================== Top-Sort by %s ====================================\n", title)
	dmlList := rankByCount(myMap)
	topNum := param.TopNum
	if len(dmlList) < param.TopNum {
		topNum = len(dmlList)
	}

	if param.InputQuery == "" {
		for i := 0; i < topNum; i++ {
			if dmlList[i].Value > 0 {
				fmt.Printf("<%d> %8v|   %v\n", i+1, dmlList[i].Value, dmlList[i].Key)
			}
		}
	} else {
		// fix InputQuery not match table name
		tablNameKey := SqlTextTablekey(param.InputQuery)
		j := 0
		for i := 0; i < len(dmlList); i++ {
			if dmlList[i].Value > 0 && strings.Index(strings.ToLower(dmlList[i].Key.(string)), strings.Replace(tablNameKey, "`", "", -1)) != -1 && j < topNum {
				fmt.Printf("<%d> %8v|   %v\n", j+1, dmlList[i].Value, dmlList[i].Key)
				j++
			}
		}
	}
}

func OutputPlus(param *BinlogInfo, myMap map[string]int, title string, myCountMap map[string]int, mySizeMap map[string]int) {
	if len(myMap) == 0 {
		return
	}
	fmt.Printf("\n==================================== Top-Sort by %s ====================================\n", title)
	dmlList := rankByCount(myMap)
	topNum := param.TopNum
	if len(dmlList) < param.TopNum {
		topNum = len(dmlList)
	}

	fmt.Printf("        DMLs|  %v DML| DML-Trans| DML-Per-Tran| Byte-Per-DML\n", strings.Repeat(" ", 57))
	fmt.Println(strings.Repeat("-", 115))
	if param.InputQuery == "" {
		for i := 0; i < topNum; i++ {
			if dmlList[i].Value > 0 {
				fmt.Printf("<%d> %8v|   %60v|  %8v|  %11d|  %11d\n", i+1, dmlList[i].Value, dmlList[i].Key, myCountMap[dmlList[i].Key.(string)], dmlList[i].Value/myCountMap[dmlList[i].Key.(string)], mySizeMap[dmlList[i].Key.(string)]/dmlList[i].Value)
			}
		}
	} else {
		// fix InputQuery not match table name
		tablNameKey := SqlTextTablekey(param.InputQuery)
		j := 0
		for i := 0; i < len(dmlList); i++ {
			if dmlList[i].Value > 0 && strings.Index(strings.ToLower(dmlList[i].Key.(string)), strings.Replace(tablNameKey, "`", "", -1)) != -1 && j < topNum {
				fmt.Printf("<%d> %8v|   %60v|  %8v|  %11d|  %11d\n", j+1, dmlList[i].Value, dmlList[i].Key, myCountMap[dmlList[i].Key.(string)], dmlList[i].Value/myCountMap[dmlList[i].Key.(string)], mySizeMap[dmlList[i].Key.(string)]/dmlList[i].Value)
				j++
			}
		}
	}
}

func OutputStmtInfo(param *BinlogInfo, myStmt []StmtInfo, title string) {
	if len(myStmt) == 0 {
		return
	}
	fmt.Printf("\n==================================== Top-Sort by %s ====================================\n", title)
	dmlList := rankByCountStmtInfo(myStmt)
	topNum := param.TopNum
	if len(dmlList) < param.TopNum {
		topNum = len(dmlList)
	}

	if param.InputQuery == "" {
		//two variable type share same interface
		for i := 0; i < topNum; i++ {
			if dmlList[i].Value > 0 {
				tm := time.Unix(int64(dmlList[i].Dt), 0)
				switch dmlList[0].Key.(type) {
				case string:
					fmt.Printf("<%d> %8v|  %v|   %v\n", i+1, dmlList[i].Value, tm, dmlList[i].Key)
				case map[string]int:
					fmt.Printf("<%d> %8v|  %v|     %d| %v\n", i+1, dmlList[i].Value, tm, len(dmlList[i].Key.(map[string]int)), dmlList[i].Key)
				}
			}
		}
	} else {
		j := 0
		matchFlag := 0
		//two variable type share same interface
		switch dmlList[0].Key.(type) {
		case string:
			for i := 0; i < len(dmlList); i++ {
				// match keyword
				if dmlList[i].Value > 0 && strings.Index(strings.ToLower(dmlList[i].Key.(string)), param.InputQuery) != -1 && j < topNum {
					tm := time.Unix(int64(dmlList[i].Dt), 0)
					fmt.Printf("<%d> %8v|  %v|   %v\n", j+1, dmlList[i].Value, tm, dmlList[i].Key)
					j++
				}
			}

		case map[string]int:
			for i := 0; i < len(dmlList); i++ {
				if dmlList[i].Value > 0 && j < topNum {
					// interface convert to map[string]int
					for k := range dmlList[i].Key.(map[string]int) {
						// match keyword
						if strings.Index(strings.ToLower(k), param.InputQuery) != -1 {
							matchFlag = 1
							break
						}
					}
					if matchFlag == 1 {
						tm := time.Unix(int64(dmlList[i].Dt), 0)
						fmt.Printf("<%d> %8v|  %v|     %d| %v\n", j+1, dmlList[i].Value, tm, len(dmlList[i].Key.(map[string]int)), dmlList[i].Key)
						matchFlag = 0
						j++
					}
				}
			}
		}
	}
}

func AutoServerId() uint32 {
	rand.Seed(time.Now().UnixNano())
	return uint32(rand.Intn(100) + 1000000)
}

type StmtInfo struct {
	Key   interface{}
	Value int
	Dt    uint32
}

type BinlogInfo struct {
	Cfg            BinlogSyncerConfig
	BinlogName     string
	TopNum         int
	MaxSqllen      int
	InputStartTime uint32
	InputEndTime   uint32
	InputQuery     string
}

func InputInfo() *BinlogInfo {
	var ip = flag.String("ip", "127.0.0.1", "MySQL IP default:127.0.0.1")
	var port = flag.Int("port", 3306, "MySQL Port default:3306")
	var binlogName = flag.String("binlog", "", "binlog must in xxxx-bin.index")
	var topNum = flag.Int("top", 5, "Print Top Count")
	var maxSqllen = flag.Int("len", 300, "Print max-length of sql statement")
	var inputStartTime = flag.String("starttime", "", "Input start time")
	var inputEndTime = flag.String("endtime", "", "Input end time")
	/*
		-query '`table`' ok
		-query 'update `table`' ok
		` must be use '`'  not "`"
	*/
	var inputQuery = flag.String("query", "", "-query '`table`' ok| -query 'update `table`' ok")
	flag.Parse()

	if *binlogName == "" {
		log.Fatal("Error, Please input like: ./binlogspy  -binlog binlogName -ip 127.0.0.1 -port 3306 -top 10 -len 300 -starttime \"\" -endtime \"\" \n")
	}

	var unixStartTime int64
	var unixEndTime int64

	if *inputStartTime != "" && *inputEndTime != "" {
		loc, _ := time.LoadLocation("Local")
		TmpStartTime, err := time.ParseInLocation("2006-01-02 15:04:05", *inputStartTime, loc)
		if err != nil {
			log.Fatal("Error, start-time input wrong!")
		}
		unixStartTime = TmpStartTime.Unix()

		TmpEndTime, err := time.ParseInLocation("2006-01-02 15:04:05", *inputEndTime, loc)
		if err != nil {
			log.Fatal("Error, end-time input wrong!")
		}
		unixEndTime = TmpEndTime.Unix()

		if unixEndTime-unixStartTime <= 0 {
			log.Fatal("Error, end-time input wrong!")
		}

	} else {
		unixStartTime = 0
		unixEndTime = 0
	}

	cfg := BinlogSyncerConfig{
		ServerID: AutoServerId(),
		Flavor:   "mysql",
		Host:     *ip,
		Port:     uint16(*port),
		User:     "",
		Password: "",
	}

	var binlog = new(BinlogInfo)
	binlog.Cfg = cfg
	binlog.BinlogName = *binlogName
	binlog.TopNum = *topNum
	binlog.MaxSqllen = *maxSqllen
	binlog.InputStartTime = uint32(unixStartTime)
	binlog.InputEndTime = uint32(unixEndTime)
	binlog.InputQuery = strings.ToLower(*inputQuery)
	return binlog
}

func wrongSql(sql string) string {
	sqlNew := sql
	if sql[:7] == "update " {
		sqlNew = sql + " set neverrun=1 where 1=0"

	} else if sql[:7] == "insert " {
		if strings.Index(sql, " into ") != -1 {
			sqlNew = sql + " values (1)"
		} else {
			sqlNew = "insert into " + sql[7:] + " values (-1)"
		}

	} else if sql[:7] == "delete " {
		if strings.Index(sql, " from ") != -1 {
			sqlNew = sql + " where 1=0"
		} else {
			sqlNew = "delete from " + sql[7:] + " where 1=0"
		}
	}
	//fmt.Println("new", sqlNew)
	stmt, err := sqlparser.Parse(sqlNew)
	if err != nil {
		return sql
	}
	return correctSql(sql, stmt)
}

func correctSql(sql string, stmt sqlparser.Statement) string {
	if sql[:7] == "update " {
		return stmt.(*sqlparser.Update).TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
	} else if sql[:7] == "insert " {
		return stmt.(*sqlparser.Insert).Table.Name.String()
	} else if sql[:7] == "delete " {
		return stmt.(*sqlparser.Delete).TableExprs[0].(*sqlparser.AliasedTableExpr).Expr.(sqlparser.TableName).Name.String()
	}
	return sql
}

func SqlTextTablekey(sql string) string {
	sql = strings.TrimSpace(sql)
	if len(sql) < 7 {
		return sql
	}
	//fmt.Println("old", sql)
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return wrongSql(sql)
	}
	return correctSql(sql, stmt)
}

