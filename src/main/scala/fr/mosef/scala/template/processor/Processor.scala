// src/main/scala/fr/mosef/scala/template/processor/Processor.scala

package fr.mosef.scala.template.processor

import org.apache.spark.sql.DataFrame

trait Processor {

  // Total des montants par client
  def totalMontantParClient(inputDF: DataFrame): DataFrame

  // Date du premier crédit par client
  def firstCreditDate(dataFrame: DataFrame): DataFrame

  // Montant moyen par type de crédit
  def montantMoyenParType(dataFrame: DataFrame): DataFrame

  // Fonction process complète pour Main.scala
  def process(inputDF: DataFrame): (DataFrame, DataFrame, DataFrame)
}
