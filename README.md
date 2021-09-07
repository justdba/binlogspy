# binlog-spy

binlogspy对binlog进行多维度统计，可提供如下功能，协助排查相关问题  
1、按热表排名  
2、按DML排名  
3、按SQL执行次数排名  
4、按SQL大小排名  
5、按SQL耗时排名  
6、按事务大小排名  
7、按事务耗时排名  
  




# 输入json参数
ip_business: MySQL实例物理ip  
port: MySQL实例端口  
binlog: 待分析的binlog文件  
mode: offline离线分析 / online在线分析  
starttime: binlog起始时间  
endtime: binlog结束时间  
query: keywords

  
# 输出结果
==================================== Top-Sort by Table Change Times ==================  
<1>  287397|    xxdb.table11  
  
==================================== Top-Sort by Table DML Times ==================  
   DML-Times|                                                    DML|Tran-Times|DML-Per-Tran  
<1>   287397|                                   xxxdb.table11.update|   2287397|           1   
  
==================================== Top-Sort by SQL Times ==================  
<1>  287397|  2021-05-11 11:54:33|    update table11 set modify_time=? where key=? and env=?  
  
==================================== Top-Sort by Large SQL (bytes) ==================  
<1>    1082|    2021-05-11 11:54:33|     replace into table11(col1,col2) values  
  
==================================== Top-Sort by Long SQL (secs) ==================  
  
==================================== Top-Sort by large Trans (bytes) ==================  
<1>    9280|     2021-05-11 11:54:33|     10|map[replace into table11(col1,col2) values ('bbb','aaa')]
==================================== Top-Sort by Long Trans (secs) ==================  
  


# 注意事项
binlog文件所在路径必须为/data/mysql/ 或 /data/mysqlxxxxx/下，其中offline模式可以支持归档日志的分析
