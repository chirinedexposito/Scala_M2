// src/main/scala/fr/mosef/scala/template/reader/Reader.scala

package fr.mosef.scala.template.reader

import org.apache.spark.sql.DataFrame

trait Reader {

  def read(format: String, options: Map[String, String], path: String): DataFrame
  def read(path: String): DataFrame
  def readParquet(path: String): DataFrame
  def readTable(tableName: String, location: String): DataFrame
  def read(): DataFrame
  def getInputPathFromProperties(): String
  def getOutputPathFromProperties(): String
  def readFromProperties(): DataFrame
}
