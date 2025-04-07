package fr.mosef.scala.template

import org.apache.spark.sql.{DataFrame, SparkSession}
import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.SparkConf
import com.globalmentor.apache.hadoop.fs.BareLocalFileSystem
import org.apache.hadoop.fs.FileSystem

import java.util.Properties
import java.io.{FileInputStream, InputStream}

object Main extends App {

  val cliArgs = args

  val MASTER_URL: String = try cliArgs(0) catch {
    case _: ArrayIndexOutOfBoundsException => "local[1]"
  }

  val SRC_PATH: String = try cliArgs(1) catch {
    case _: ArrayIndexOutOfBoundsException =>
      println("❌ Aucune source d'entrée définie")
      sys.exit(1)
  }

  val DST_PATH: String = try cliArgs(2) catch {
    case _: ArrayIndexOutOfBoundsException => "./default/output-writer"
  }

  val REPORT_TYPES: Seq[String] = try cliArgs(3).split(",").map(_.trim).toSeq catch {
    case _: ArrayIndexOutOfBoundsException =>
      println("⚠️ Aucun type de rapport précisé, 'report1' utilisé par défaut")
      Seq("report1")
  }

  val CONFIG_PATH: Option[String] = if (cliArgs.length > 4) Some(cliArgs(4)) else None

  val conf = new SparkConf()
  conf.set("spark.driver.memory", "2g")
  conf.set("spark.testing.memory", "471859200")

  val sparkSession = SparkSession
    .builder
    .master(MASTER_URL)
    .config(conf)
    .appName("Scala Template")
    .enableHiveSupport()
    .getOrCreate()

  sparkSession.sparkContext.hadoopConfiguration.setClass(
    "fs.file.impl",
    classOf[BareLocalFileSystem],
    classOf[FileSystem]
  )

  def detectFormatFromPath(path: String): String = {
    val lower = path.toLowerCase
    if (lower.endsWith(".csv")) "csv"
    else if (lower.endsWith(".parquet")) "parquet"
    else if (lower.startsWith("hive:")) "hive"
    else "unknown"
  }

  // ✅ Chargement automatique de la config
  val confWriter = new Properties()
  val stream: InputStream = CONFIG_PATH match {
    case Some(path) =>
      println(s"📄 Chargement config externe : $path")
      new FileInputStream(path)
    case None =>
      println("📄 Chargement config interne : configuration.properties (dans resources)")
      val resourceStream = getClass.getClassLoader.getResourceAsStream("configuration.properties")
      if (resourceStream == null) {
        throw new RuntimeException("❌ Fichier de configuration interne introuvable !")
      }
      resourceStream
  }

  confWriter.load(stream)

  val reader: Reader = new ReaderImpl(sparkSession)
  val processor: Processor = new ProcessorImpl()
  val writer: Writer = new Writer(sparkSession, confWriter)

  val format = detectFormatFromPath(SRC_PATH)
  println(s"📦 Format détecté: $format")

  val inputDF: DataFrame = format match {
    case "csv" => reader.readCSV(SRC_PATH, delimiter = ",", header = true)
    case "parquet" => reader.readParquet(SRC_PATH)
    case "hive" => reader.readHiveTable(SRC_PATH.stripPrefix("hive:"))
    case _ =>
      println(s"❌ Format inconnu pour le chemin : $SRC_PATH")
      sys.exit(1)
  }

  REPORT_TYPES.foreach { report =>
    val processedDF = processor.process(inputDF, report)
    val outputPath = s"$DST_PATH/$report"
    println(s"📝 Écriture du rapport '$report' vers $outputPath")
    writer.write(processedDF, outputPath)
  }
}
