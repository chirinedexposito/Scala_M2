// src/main/scala/fr/mosef/scala/template/writer/impl/WriterImpl.scala

package fr.mosef.scala.template.writer.impl
import java.io.FileNotFoundException
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.DataFrame
import java.io.FileInputStream
import java.util.Properties

class WriterImpl(propertiesFilePath: String) extends Writer {

  val properties: Properties = new Properties()

  try {
    val inputStream = getClass.getClassLoader.getResourceAsStream(propertiesFilePath)
    if (inputStream == null) throw new FileNotFoundException(s"Fichier de propriétés introuvable : $propertiesFilePath")
    properties.load(inputStream)
    println(s"✅ Fichier de configuration chargé depuis le classpath : $propertiesFilePath")
  } catch {
    case e: Exception =>
      println(s"❌ Erreur lors du chargement de $propertiesFilePath")
      throw e
  }

  def writeCSV(df: DataFrame, path: String, mode: String = "overwrite"): Unit = {
    df.write
      .option("header", properties.getProperty("write_header", "true"))
      .option("sep", properties.getProperty("write_separator", ";"))
      .mode(mode)
      .csv(path)
  }

  def writeParquet(df: DataFrame, path: String, mode: String = "overwrite"): Unit = {
    df.write
      .mode(mode)
      .parquet(path)
  }

  def writeTable(df: DataFrame, tableName: String, tablePath: String, mode: String = "overwrite"): Unit = {
    df.write
      .mode(mode)
      .option("path", tablePath)
      .saveAsTable(tableName)
  }

  def write(df: DataFrame, mode: String, path: String, filename: String): Unit = {
    val format = properties.getProperty("write_format", "csv")
    val fullPath = s"$path/$filename"

    format match {
      case "csv"     => writeCSV(df, fullPath, mode)
      case "parquet" => writeParquet(df, fullPath, mode)
      case _ => throw new IllegalArgumentException(s"Format non supporté : $format")
    }
  }
}
