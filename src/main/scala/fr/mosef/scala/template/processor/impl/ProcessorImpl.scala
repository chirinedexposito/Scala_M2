// src/main/scala/fr/mosef/scala/template/processor/impl/ProcessorImpl.scala

package fr.mosef.scala.template.processor.impl

import fr.mosef.scala.template.processor.Processor
import org.apache.spark.sql.{DataFrame, functions => F}

class ProcessorImpl() extends Processor {

  // Total des montants par client
  def totalMontantParClient(inputDF: DataFrame): DataFrame = {
    inputDF.groupBy("client_id")
      .agg(F.sum("montant").alias("total_montant"))
  }

  // Date du premier crédit par client
  def firstCreditDate(df: DataFrame): DataFrame = {
    df.groupBy("client_id")
      .agg(F.min("date_ouverture").alias("premier_credit"))
  }

  // Montant moyen par type de crédit
  def montantMoyenParType(df: DataFrame): DataFrame = {
    df.groupBy("type_credit")
      .agg(F.avg("montant").alias("montant_moyen"))
  }

  // Regroupe les 3 transformations en une seule méthode
  def process(inputDF: DataFrame): (DataFrame, DataFrame, DataFrame) = {
    val report1 = totalMontantParClient(inputDF)
    val report2 = firstCreditDate(inputDF)
    val report3 = montantMoyenParType(inputDF)
    (report1, report2, report3)
  }

}
