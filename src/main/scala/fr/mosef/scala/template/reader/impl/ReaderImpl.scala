// src/main/scala/fr/mosef/scala/template/reader/impl/ReaderImpl.scala

package fr.mosef.scala.template.reader.impl

import java.io.{FileNotFoundException, InputStream}
import java.util.Properties
import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.reader.Reader

class ReaderImpl(sparkSession: SparkSession, propertiesFilePath: String) extends Reader {

  val properties: Properties = new Properties()

  try {
    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream(propertiesFilePath)
    if (inputStream == null) throw new FileNotFoundException(s"Fichier de propriétés introuvable dans le classpath : $propertiesFilePath")
    properties.load(inputStream)
    println(s"✅ Chargement des propriétés depuis le classpath : $propertiesFilePath")
  } catch {
    case e: Exception =>
      println(s"❌ Erreur : Impossible de charger $propertiesFilePath")
      throw e
  }

  def read(format: String, options: Map[String, String], path: String): DataFrame = {
    sparkSession
      .read
      .options(options)
      .format(format)
      .load(path)
  }

  def read(path: String): DataFrame = {
    sparkSession
      .read
      .option("sep", properties.getProperty("read_separator", ","))
      .option("inferSchema", properties.getProperty("schema", "true"))
      .option("header", properties.getProperty("read_header", "true"))
      .format(properties.getProperty("read_format_csv", "csv"))
      .load(path)
  }

  def readParquet(path: String): DataFrame = {
    sparkSession
      .read
      .format(properties.getProperty("read_format_parquet", "parquet"))
      .load(path)
  }

  def readTable(tableName: String, location: String): DataFrame = {
    sparkSession
      .read
      .format("parquet")
      .option("basePath", location)
      .load(location + "/" + tableName)
  }

  def read(): DataFrame = {
    sparkSession.sql("SELECT 'Empty DataFrame for unit testing implementation'")
  }

  def getInputPathFromProperties(): String = {
    val path = properties.getProperty("input_path")
    if (path == null) throw new NullPointerException("input_path n’est pas défini dans le fichier de propriétés.")
    path
  }

  def getOutputPathFromProperties(): String = {
    val path = properties.getProperty("output_path")
    if (path == null) throw new NullPointerException("output_path n’est pas défini dans le fichier de propriétés.")
    path
  }

  def readFromProperties(): DataFrame = {
    read(getInputPathFromProperties())
  }
}
