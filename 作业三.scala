// https://blog.csdn.net/nzbing/article/details/126082462

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

case class MyRule(spark: SparkSession) extends Rule[LogicalPlan] {
  logWarning("我爱夜来香A的规则")
  override def apply(plan: LogicalPlan): LogicalPlan = plan
}

import org.apache.spark.sql.SparkSessionExtensions

class MySparkSessionExtension extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule {
      session => MyRule(session)
    }
  }
}
