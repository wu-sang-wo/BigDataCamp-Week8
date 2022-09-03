// https://blog.csdn.net/nzbing/article/details/124653021
// SqlParsqlParser.scala

// 增加visitShowVersion方法
override def visitShowVersion(ctx: ShowVersionContext): LogicalPlan = withOrigin(ctx) {
    ShowVersionCommand()
}


// 实现ShowVersionCommand类
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.StringType


case class ShowVersionCommand() extends LeafRunnableCommand {

  override val output: Seq[Attribute] =
    Seq(AttributeReference("version", StringType)())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sparkVersion = sparkSession.version
    val javaVersion = System.getProperty("java.version")
    val scalaVersion = scala.util.Properties.releaseVersion
    val output = "Spark Version: %s, Java Version: %s, Scala Version: %s"
      .format(sparkVersion, javaVersion, scalaVersion.getOrElse(""))
    Seq(Row(output))
  }
}
