// src/main/scala/Main.scala

import fr.mosef.scala.template.reader.impl.ReaderImpl
import fr.mosef.scala.template.processor.impl.ProcessorImpl
import fr.mosef.scala.template.writer.impl.WriterImpl
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.SparkConf

object Main {
  def main(args: Array[String]): Unit = {
    println("🚀 Démarrage de l'application Scala Spark...")

    val conf = new SparkConf()
      .setAppName("Scala Template")
      .setMaster("local[*]")

    val sparkSession = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // Chargement du fichier de configuration depuis le classpath
    val propertiesFile = "configuration.properties"

    // Initialisation des composants
    println("🔧 Initialisation des composants Reader / Processor / Writer...")
    val reader = new ReaderImpl(sparkSession, propertiesFile)
    val processor = new ProcessorImpl()
    val writer = new WriterImpl(propertiesFile)

    val inputDF = reader.readFromProperties()
    val dst_path = reader.getOutputPathFromProperties()

    println("⚙️  Lancement du traitement des données...")
    val (report1, report2, report3) = processor.process(inputDF)

    println("✅ Traitement terminé.")
    println("📤 Écriture des résultats dans les fichiers de sortie...")

    writer.write(report1, "overwrite", dst_path + "_report1", "total_montants_par_client")
    println("📝 Rapport 1 écrit : total_montants_par_client")

    writer.write(report2, "overwrite", dst_path + "_report2", "premier_credit_par_client")
    println("📝 Rapport 2 écrit : premier_credit_par_client")

    writer.write(report3, "overwrite", dst_path + "_report3", "montant_moyen_par_type")
    println("📝 Rapport 3 écrit : montant_moyen_par_type")

    println("🎉 Pipeline terminé avec succès !")
    sparkSession.stop()
  }
}
