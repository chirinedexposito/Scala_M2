package fr.mosef.scala.template.job

import fr.mosef.scala.template.processor.Processor
import fr.mosef.scala.template.reader.Reader
import fr.mosef.scala.template.writer.Writer
import org.apache.spark.sql.{DataFrame, SparkSession}

class Job(reader: Reader, processor: Processor, writer: Writer)(implicit spark: SparkSession) {

  println("🔧 Initialisation du job...")

  // Chemins input/output
  val src_path: String = reader.getInputPathFromProperties()
  val dst_path: String = reader.getOutputPathFromProperties()

  // Lecture des données
  val inputDF: DataFrame = reader.readFromProperties()

  // Traitement des données
  val (report1, report2, report3): (DataFrame, DataFrame, DataFrame) = processor.process(inputDF)

  // Écriture des résultats
  writer.write(report1, "overwrite", dst_path, "total_montants_par_client")
  writer.write(report2, "overwrite", dst_path, "premier_credit_par_client")
  writer.write(report3, "overwrite", dst_path, "montant_moyen_par_type")

  println("🎉 Job terminé avec succès !")
}
