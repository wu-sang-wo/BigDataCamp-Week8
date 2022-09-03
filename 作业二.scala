// https://blog.csdn.net/nzbing/article/details/124649403

import org.apache.spark.sql.DataFrame
val dfOracle:DataFrame = spark.read.format("jdbc").option("driver", "oracle.jdbc.driver.OracleDriver").option("url", "jdbc:oracle:thin:@192.168.0.101:1521:orcl").option("user", "scott").option("password","scott").option("numPartitions", 20).option("dbtable", "EMP").load()
//连接scott用户中emp表进行测试
//创建临时表
dfOracle.createTempView("t")
//使得apply哪些规则打印出来
spark.sql("SET spark.sql.planChangeLog.level=WARN")

// 三条优化规则
val query:String = "SELECT a.ename FROM (SELECT ename, sal FROM t WHERE 1 = 1 AND sal > 1000) AS a WHERE a.sal < 2000)
spark.sql(query).explain(true)

// 五条优化规则
//oracle库中新建一张表
CREATE TABLE emp1 AS SELECT * FROM emp WHERE deptno = 20;
//创建DataFrame
val dfOracle1:DataFrame = spark.read.format("jdbc").option("driver", "oracle.jdbc.driver.OracleDriver").option("url", "jdbc:oracle:thin:@192.168.0.101:1521:orcl").option("user", "scott").option("password","scott").option("numPartitions", 20).option("dbtable", "EMP").load()
//创建临时表
dfOracle1.createTempView("t1")
val query:String = "SELECT DISTINCT a.job FROM (SELECT * FROM t WHERE sal > 1000  and 1 = 1) AS a WHERE a.deptno = 10 EXCEPT SELECT job FROM t1 WHERE sal > 2000"
spark.sql(query).explain(true)
