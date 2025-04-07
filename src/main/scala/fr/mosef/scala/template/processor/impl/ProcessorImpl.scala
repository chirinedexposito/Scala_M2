package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.functions.col

class ProcessorImpl extends Processor {

  override def process(inputDF: DataFrame, reportType: String): DataFrame = {
    reportType match {
      case "report1" => totalMontantParClient(inputDF)
      case "report2" => firstCreditDate(inputDF)
      case "report3" => montantMoyenParType(inputDF)
      case _ =>
        throw new IllegalArgumentException(s"Type de rapport inconnu : $reportType")
    }
  }

  // Total des montants par client
  def totalMontantParClient(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("client_id")
      .agg(F.sum("montant").alias("total_montant"))
  }


  // Date du premier crédit par client
  def firstCreditDate(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("client_id")
      .agg(F.min("date_ouverture").alias("premier_credit"))
  }


  // Montant moyen par type de crédit
  def montantMoyenParType(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("type_credit")
      .agg(F.avg("montant").alias("montant_moyen"))
  }

}