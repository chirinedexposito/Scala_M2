// src/main/scala/fr/mosef/scala/template/writer/Writer.scala

package fr.mosef.scala.template.writer

import org.apache.spark.sql.DataFrame

trait Writer {
  def writeCSV(df: DataFrame, path: String, mode: String = "overwrite"): Unit
  def writeParquet(df: DataFrame, path: String, mode: String = "overwrite"): Unit
  def writeTable(df: DataFrame, tableName: String, tablePath: String, mode: String = "overwrite"): Unit
  def write(df: DataFrame, mode: String, path: String, filename: String): Unit
}
