package app.main

import app.common.base.{InArgs, SparkApp}
import app.common.executor.Executor
import app.common.template.DbToPq
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.bson.Document
import spark.common.util.HBTV

object WorkitemEditsphere extends SparkApp {
  def exec(implicit args: InArgs) = {
    compo.run()
  }

  val compo = new DbToPq with Executor {
    val compoName = "id001"
    val writerPqName: String = "wf_item_hbtv"
    val writeMode = Overwrite
    val sourceStation = new HBTV
    val readTableName = "nSite.wf.workitems"
    override val schema = StructType(
      List(
        StructField("_id", StringType, true),
        StructField("type", StringType, true),
        StructField("activityDefineName", StringType, true),
        StructField("activityTemplateName", StringType, true),
        StructField("worker", StringType, true),
        StructField("state", StringType, true),
        StructField("createdTime", StringType, true),
        StructField("createdBy", StringType, true),
        StructField("startTime", StringType, true),
        StructField("stopTime", StringType, true),
        StructField("name", StringType, true)
      )
    )
    override val matchQuery = Document.parse("{ $match: { state : { $in : ['Terminated','Completed','Exception']} } } ")

    def invoke(df: DataFrame)(implicit args: InArgs) = {
      val pdf = df
        .withColumn("sourceSystem", lit("editsphere_v1"))
        .withColumn("sourceStation", lit("HBTV"))
        .withColumn("name", editName(col("name")))
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
}
