/**
  * (C) Copyright IBM Corporation 2020.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
  * the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package io.fybrik.mover.spark

import org.apache.spark.scheduler._
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.ui.{SparkListenerDriverAccumUpdates, SparkListenerSQLExecutionEnd, SparkListenerSQLExecutionStart}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.util.Try

/**
  * This class is a Spark counter tool that can be added on top of any [[DataFrame]].
  * It will use the Spark internal counters to count the number of objects written to an output.
  * It's based on the [[SparkListener]] interface and accumulators.
  * Currently it has been tested with parquet output.
  *
  * Before usage this counter has to be registered with the current [[SparkSession]].
  */
class SparkOutputCounter extends SparkListener {

  private val logger = LoggerFactory.getLogger(getClass)

  private val watchPaths = new mutable.HashSet[String]()
  private val keyWorkdToExecutionId = new mutable.HashMap[String, Long]()
  private val executionIdToAccumulatorID = new mutable.HashMap[Long, Long]()
  private val pathToAccumulatorIDMap: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()
  private val accumulatorIDToValueMap: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]()
  private val collectedAccumulators: mutable.HashMap[String, Long] = new mutable.HashMap[String, Long]()

  def reset(): Unit = {
    watchPaths.clear()
    pathToAccumulatorIDMap.clear()
    accumulatorIDToValueMap.clear()
  }

  def getCounts: Map[String, Long] = {
    pathToAccumulatorIDMap.flatMap { case (path, accumulatorId) => accumulatorIDToValueMap.get(accumulatorId).map(v => (path, v)) }(collection.breakOut)
  }

  /**
    * Registers this counter instance with the given [[SparkSession]].
    * @param spark spark session
    */
  def register(spark: SparkSession): Unit = {
    spark.sparkContext.addSparkListener(this)
  }

  /**
    * Unregisters this counter instance with the given [[SparkSession]].
    * @param spark spark session
    */
  def unregister(spark: SparkSession): Unit = {
    spark.sparkContext.removeSparkListener(this)
  }

  /**
    * Registers the given [[DataFrame]] with this counter.
    * It creates an alias with the given group name in order to match it later on in the execution plan.
    * Please use the return value of this method to continue!
    *
    * @param dataframe dataframe to register
    * @param groupName counter name to register
    * @return
    */
  def registerDataframe(dataframe: DataFrame, groupName: String): DataFrame = {
    watchPaths.add(groupName)
    dataframe.alias(groupName)
  }

  /**
    * Return the value of a given group name if available.
    *
    * @param groupName counter name
    * @return
    */
  def getValue(groupName: String): Option[Long] = {
    Try(accumulatorIDToValueMap(pathToAccumulatorIDMap(groupName))).toOption
  }

  private def findAccumulatorId(sparkPlan: SparkPlanInfo): Option[Long] = {
    logger.debug("Found metrics: " + sparkPlan.metrics.mkString(","))
    if (sparkPlan.metrics.exists(_.name.equals("number of output rows"))) {
      Some(sparkPlan.metrics.filter(_.name.equals("number of output rows")).head.accumulatorId)
    } else {
      sparkPlan.children.flatMap(findAccumulatorId).headOption
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: SparkListenerSQLExecutionStart =>
        logger.debug("Physical Plan description: " + e.physicalPlanDescription)
        logger.debug("NodeName: " + e.sparkPlanInfo.nodeName)
        logger.debug("SimpleString: " + e.sparkPlanInfo.simpleString)
        watchPaths.foreach { watchedPath =>
          if (e.physicalPlanDescription.contains(s"SubqueryAlias $watchedPath\n") || // Spark 2.3.x
            e.physicalPlanDescription.contains(s"SubqueryAlias `$watchedPath`") // Spark 2.4.x
            ) {
            logger.debug(s"Found match for watch path $watchedPath!!")
            e.sparkPlanInfo.metrics.foreach { m => logger.debug(s"Found metric: ${m.name} of type ${m.metricType} with id ${m.accumulatorId}") }
            findAccumulatorId(e.sparkPlanInfo) match {
              case Some(accumulatorId) =>
                logger.debug(s"Found metric to watch! ID $accumulatorId")
                pathToAccumulatorIDMap.put(watchedPath, accumulatorId)
                executionIdToAccumulatorID.put(e.executionId, accumulatorId)
                keyWorkdToExecutionId.put(watchedPath, e.executionId)
              case None =>
                logger.debug("No metrics found to watch")
            }
          }
        }

      case e: SparkListenerSQLExecutionEnd =>
      case e: SparkListenerDriverAccumUpdates =>
        logger.debug("Update on accumulators!")
        e.accumUpdates.foreach { case (id, value) =>
          logger.debug("Accumulator ID: {}, Value: {}", id, value)
          accumulatorIDToValueMap.put(id, value)
        }
      case _ => // do nothing
    }
  }

  def lastNumOfOutputRows: Option[Long] = {
    collectedAccumulators.get("number of output rows")
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    stageCompleted.stageInfo.accumulables.values.foreach { a =>
      Try(collectedAccumulators.put(a.name.get, a.value.get.asInstanceOf[Long]))
    }
    stageCompleted.stageInfo.accumulables.values.foreach { i => logger.debug(s"Metric result: ${i.name} (${i.id}) = ${i.value}") }
    pathToAccumulatorIDMap.values.foreach { accumulatorId =>
      stageCompleted.stageInfo.accumulables.get(accumulatorId).foreach { info =>
        logger.debug(s"stage id: ${stageCompleted.stageInfo.stageId} id: $accumulatorId ${info.name} = ${info.value.get}")
        accumulatorIDToValueMap.put(accumulatorId, info.value.get.toString.toLong)
      }
    }
  }
}

object SparkOutputCounter {
  def createAndRegister(spark: SparkSession): SparkOutputCounter = {
    val counter = new SparkOutputCounter()
    counter.register(spark)
    counter
  }
}
