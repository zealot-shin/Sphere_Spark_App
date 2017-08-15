package app.main

import app.common.base.{InArgs, SparkApp}
import app.common.executor.Executor
import app.common.template.DbToPq
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import spark.common.util.NewSphere

object WorkitemNewsphere extends SparkApp {
  def exec(implicit args: InArgs) = {
    compo.run()
  }

  val compo = new DbToPq with Executor {
    val compoName = "id001"
    val writerPqName: String = "wf_item_newsphere"
    val writeMode = Overwrite
    val sourceStation = new NewSphere
    val readTableName = "wf_workitem"
    override val columns = Array("workitemid", "workitemtype", "activitydefname", "activitytmplname", "worker",
      "currentstate", "createtime", "createdby", "starttime", "stoptime", "workname", "'newsphere_v1'", "'CDV'")

    override val readDbWhere = Array(s"currentstate in (3,4,5)")

    def invoke(df: DataFrame)(implicit args: InArgs) = {
      val pdf = df
        .withColumn("sourceSystem", col("newsphere_v1"))
        .withColumn("sourceStation", col("CDV"))
        .withColumn("name", editName(col("workname")))
        .withColumn("state", editStateCode(col("currentstate")))
      pdf.show(5, false)
      println(pdf.count)
      pdf
    }
  }

  val editName = udf { (name: String) => {
    name match {
      case null => null
      case _ => name.replaceAll("\\||\r|\n", " ")
    }
  }
  }

  val editStateCode = udf { (code: Int) => {
    code match {
      case 3 => "Terminated"
      case 4 => "Completed"
      case 5 => "Exception"
    }
  }
  }
}
