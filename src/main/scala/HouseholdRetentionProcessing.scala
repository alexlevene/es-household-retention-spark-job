
package com.healthgrades.edp.spark

// dependencies required for elasticsearch direct access via REST client interface
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.client.BasicResponseHandler
import java.util.Date
import java.util.Calendar
import org.apache.commons.lang3.time.DateUtils
import scala.collection.mutable.WrappedArray



import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext    
import org.apache.spark.sql.SQLContext._
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.Metadata._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.types._

import org.apache.spark.sql.Row

// sample local command to run locally:
//make run-local ARGS="DEMO elasticsearch.exp-dev.io exp_rjj_1_2 10000"
//make run-local ARGS="DEMO 100.70.102.71 exp_rjj_1_2 10000"

object HouseholdRetentionProcessing {

  var esServer:String = "exp-elasticsearch.default.svc.cluster.local"
  var esWriteOperation:String = "upsert"
  var esServerPort:Int = 9200
  var esIndexName:String = "exp_v1_1_3"
  var clientCode:String = "DEMO"
  var batchSize:Int = 10000
  var defaultLogLevel = "INFO"
  var esBatchSizeBytes = "1mb"
  var esBatchSizeEntries = 1000
  var esRequestTimeout = "1m"
  var esRequestRetryCount = 3
  var restRequestTimeout = 5

  case class date_range (
      gte: Long,
      lte: Long
      )
  case class date_range_alt (
      gte: String,
      lte: String
      )
  case class household_retention_history (
      date_range: date_range,
      date_range_alt: date_range_alt,
      retained: Boolean
      )
  def main(args:Array[String]):Unit = {

    if (args.length != 4) {
      // if no args then get config from environment variables
      clientCode = scala.util.Properties.envOrElse("CLIENT_CODE", clientCode)
      esServer = scala.util.Properties.envOrElse("ES_HOST", esServer)
      esIndexName = scala.util.Properties.envOrElse("ES_INDEX", esIndexName)
      esServerPort = scala.util.Properties.envOrElse("ES_PORT", esServerPort.toString).toInt
      batchSize = scala.util.Properties.envOrElse("SPARK_PROCESS_BATCH_SIZE", batchSize.toString).toInt
      esBatchSizeBytes = scala.util.Properties.envOrElse("ES_WRITE_BATCH_SIZE_BYTES", esBatchSizeBytes)
      esBatchSizeEntries = scala.util.Properties.envOrElse("ES_WRITE_BATCH_SIZE_ENTRIES", esBatchSizeEntries.toString).toInt
      restRequestTimeout = scala.util.Properties.envOrElse("REST_REQUEST_TIMEOUT", restRequestTimeout.toString).toInt
      esRequestTimeout = scala.util.Properties.envOrElse("ES_REQUEST_TIMEOUT", esRequestTimeout)
      esRequestRetryCount = scala.util.Properties.envOrElse("ES_REQUEST_RETRY_COUNT", esRequestRetryCount.toString).toInt
    } else {
      clientCode = args(0)
      esServer = args(1)
      esIndexName = args(2)
      batchSize = args(3).toInt
    }

    println("-------------------------------- PROCESS START")

    println(s"clientCode: ${clientCode}")
    println(s"esServer: ${esServer}")
    println(s"esServerPort: ${esServerPort}")
    println(s"esIndexName: ${esIndexName}")
    println(s"batchSizeBytes: ${esBatchSizeBytes}")
    println(s"batchSizeEntries: ${esBatchSizeEntries}")
    println(s"restRequestTimeout: ${restRequestTimeout}")
    println(s"esRequestTimeout: ${esRequestTimeout}")
    println(s"esRequestRetryCount: ${esRequestRetryCount}")


    

    // Create a SparkSession
    val spark = SparkSession
      .builder()
      .appName("HouseholdRetentionProcessing")
     .config("es.net.ssl", true)
     .config("es.net.ssl.cert.allow.self.signed", true)
     .config("es.index.auto.create", false)
     .config("es.nodes", esServer)
     .config("es.nodes.wan.only", false)
     .config("es.write.operation", esWriteOperation)
     .config("es.batch.size.bytes", esBatchSizeBytes)
     .config("es.batch.size.entries", esBatchSizeEntries)
     .config("es.http.timeout", esRequestTimeout)
     .config("es.http.retries", esRequestRetryCount)
     .config("spark.rdd.compress", true)
     .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    import spark.implicits._

    // set the logging level
    spark.sparkContext.setLogLevel(scala.util.Properties.envOrElse("LOG_LEVEL", defaultLogLevel))

    processHouseholdRetentionRetained(spark,batchSize) 
    processHouseholdRetentionNotRetained(spark)

    spark.stop
    println("Done")
  }

  def getHouseholdsWithoutRetention(spark:SparkSession,clientCode:String,esIndexName:String,esHost:String,esPort:Int,resultLimit:Int = 10000) :(String,Int) = {

      val sc = spark.sparkContext
      val sqlContext = spark.sqlContext
      import sqlContext.implicits._

      val householdsWithoutRetention = s"""
      {
        "size": 0,
        "_source": ["household.household_id"],
        "query": {
          "constant_score": {
            "filter": {
              "bool": {
                "must": [
                  {"term":{"client_code" : "${clientCode}" }},
                  {
                    "has_child": {
                      "type": "encounter",
                      "query": {
                        "bool": {
                          "must": [
                            { "term": {"client_code": "${clientCode}" }},
                            { "range": {"admit_date": {"gte": "now-4y", "lte": "now"}}}
                          ]
                        }
                      }
                    }
                  },
                  {"exists": { "field": "household.household_id"} }
                ],
                "must_not": [
                  {
                    "nested": {
                      "path": "household_retention_history",
                      "query": {
                        "exists": { "field": "household_retention_history.retained"}
                      }
                    }
                  }
                ]
              }
            }
          }
        },
        "aggs": {
          "all_households": {
            "terms": {
              "field": "household.household_id",
              "size": ${resultLimit},
              "order": {"_term":"asc"}
            }
          }
        }
      }
      """

      // generate the elasticsearch request string
      
      val endPoint = "/"+ esIndexName + "/person/_search"
      val request_url = "https://" + esHost + ":" + esPort + endPoint
      
      // build the apache HTTP post request
      val post = new HttpPost(request_url)
      // set header for json request string
      val setHeaderReturn = post.setHeader("Content-type","application/json")
      // ad the json request string to the http post
      val setEntityReturn = post.setEntity(new StringEntity(householdsWithoutRetention))
      // send the request to elasticsearch
      val response = (new DefaultHttpClient).execute(post)
      // get the status -- this code doesn't check for HTTP/1.1 200 OK response but the final code should!
      val status = response.getStatusLine
      // get the json response body
      val responseBody = (new BasicResponseHandler).handleResponse(response).trim.toString
      
      // check the results to ensure there were any households returned
      val dfCount = sqlContext.read.json(sc.parallelize(responseBody::Nil)).select($"hits.total")
      
      var householdTotalHits:Int = dfCount.first().getLong(0).asInstanceOf[Int]
      if (householdTotalHits == 0) {
          return (null,householdTotalHits)
      }
      val df = sqlContext.read.json(sc.parallelize(responseBody::Nil)).select($"aggregations.all_households.buckets.key")
      val idList = df.first().getAs[WrappedArray[String]](0)
      var idlist_length:Int = idList.length
      if (idlist_length == 0) {
          return (null,idlist_length)
      }
      val idString = idList.mkString("\"","\",\n\"","\"")
      println (s"returned ${resultLimit} results")
      return (idString, idlist_length)
  } // getHouseholdsWithoutRetention

  def writeHouseholdRetentionDataToPerson(spark:SparkSession,householdRetentionFinal:org.apache.spark.sql.DataFrame,esIndexName:String,esServer:String) = {

      val sc = spark.sparkContext
      val sqlContext = spark.sqlContext
      import sqlContext.implicits._

      case class date_range (
                              gte: Long,
                              lte: Long
                            )
      case class date_range_alt (
                                  gte: String,
                                  lte: String
                                )
      case class household_retention_history (
                                    date_range: date_range,
                                    date_range_alt: date_range_alt,
                                    retained: Boolean
                                  )


      val updates = householdRetentionFinal
        .rdd
        .groupBy( z => z.getAs[String]("person_id"))
        .map(e => (
          Map(ID -> e._1),
          Map(household_retention_history -> e._2.map(
            a => household_retention_history(
              date_range(
                a.getAs[Long]("start_date_epoch"),
                a.getAs[Long]("end_date_epoch")),
              date_range_alt(
                a.getAs[String]("start_date"),
                a.getAs[String]("end_date")),
              a.getAs[Boolean]("retained")
            )
          ).toArray)
        ))

      var esconf = Map(
        "es.write.operation" -> "upsert"
        )

      // write elasticsearch data back to the index
      updates.saveToEsWithMeta(s"${esIndexName}/person", esconf)
  } // writeHouseholdRetentionDataToPerson

  def processHouseholdRetentionRetained(spark:SparkSession, resultLimit:Int = 10000){

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._    

    def getEncounterSourceData(spark:SparkSession,clientCode:String,esIndexName:String,esServer:String) :org.apache.spark.sql.DataFrame = {

        val encounterSourceQuery = s"""
          {
            "query": {
              "constant_score": {
                "filter": {
                  "bool": {
                    "must": [
                      {"term":{"client_code" : "${clientCode}" }},
                      { "range": {"admit_date": {"gte": "now-5y", "lte": "now"}}},
                      {
                        "has_parent": {
                          "parent_type": "person",
                          "query": {
                            "bool": {
                              "must": [
                                { "term": {"client_code": "${clientCode}" }},
                                { "exists": { "field": "household.household_id"} }
                              ],
                              "must_not": [
                                {
                                  "nested": {
                                    "path": "household_retention_history",
                                    "query": {
                                      "exists": { "field": "household_retention_history.retained"}
                                    }
                                  }
                                }
                              ]
                            }
                          }
                        } 
                      }
                    ]
                  }
                }
              }
            }
          }
        """

        // limit the fields that are included
        val encounterSourceQueryOptions = Map(
          "es.read.source.filter" -> "admit_date",
          "es.read.metadata" -> "true"
        )

        val allEncountersWithHouseholdRDD = EsSpark.esJsonRDD(sc, s"${esIndexName}/encounter", encounterSourceQuery, encounterSourceQueryOptions)

        // get jsonMap of response as second item of tuple
        val encountersJson = allEncountersWithHouseholdRDD.map[String](d => d._2)
        encountersJson.cache
        
        // read json docs into dataframe
        val allEncountersWithHouseholdDF = sqlContext.read.json(encountersJson)
        //allEncountersMissingFlagsDF.cache
        // val encounterCount = allEncountersMissingFlagsDF.count
        // println(s"total encounter count for persons with patient lifecycle not present: ${encounterCount}")

        // select and rename fields
        val encountersES = allEncountersWithHouseholdDF
        .withColumn("admit_date_ts", allEncountersWithHouseholdDF("admit_date").cast(LongType))
        .select(
          $"_metadata._id".as("encounter_id"),
          $"_metadata._parent".as("person_id"),
          $"admit_date_ts".as("admit_date_ts")
          )
        //encountersES.cache
        encountersES.createOrReplaceTempView("encountersES")
        encountersES.cache
        return encountersES
    } // getEncounterSourceData

    def getPersonSourceData(spark:SparkSession,clientCode:String,esIndexName:String,esServer:String) :org.apache.spark.sql.DataFrame = {

        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._    

        val personSourceQuery = s"""
          {
            "query": {
              "constant_score": {
                "filter": {
                  "bool": {
                    "must": [
                      {"term":{"client_code" : "${clientCode}" }},
                      { "exists": { "field": "household.household_id"} }
                    ],
                    "must_not": [
                      {
                        "nested": {
                          "path": "household_retention_history",
                          "query": {
                            "exists": { "field": "household_retention_history.retained"}
                          }
                        }
                      }
                    ]
                  }
                }
              }
            }
          }
        """


        val personSourceQueryOptions = Map(
          "es.read.source.filter" -> "household.household_id",
          "es.read.metadata" -> "true"
        )
        
        val allPersonsWithHouseholdsRDD = EsSpark.esJsonRDD(sc, s"${esIndexName}/person", personSourceQuery, personSourceQueryOptions)
        
        // get jsonMap of response as second item of tuple
        val personsJson = allPersonsWithHouseholdsRDD.map[String](d => d._2)
        personsJson.cache
        
        // read json docs into dataframe
        val allPersonsWithHouseholdsDF = sqlContext.read.json(personsJson)

        // select and rename fields
        val personsES = allPersonsWithHouseholdsDF.select(
          $"_metadata._id".as("person_id"),
          $"household.household_id".as("household_id"))
        personsES.createOrReplaceTempView("personsES")
        personsES.cache
        return personsES
    } // getPersonSourceData

    def getRetentionMonthRange(spark:SparkSession) :org.apache.spark.sql.DataFrame = {

        def monthIterator(start: org.joda.time.LocalDate, end: org.joda.time.LocalDate) = Iterator.iterate(start)(_ plusMonths 1) takeWhile (_ isBefore end)

        val max_admit_month = (new org.joda.time.LocalDate).withDayOfMonth(1)
        val min_admit_month = max_admit_month.plusYears(-4)

        val stringMonthIterator = monthIterator(min_admit_month,max_admit_month.plusMonths(1)).map(x => x.toString())

        val stringMonthList = stringMonthIterator.toList

        val monthRangeFrame = spark.createDataFrame(stringMonthList.map(Tuple1(_))).toDF("startOfMonth")

        monthRangeFrame.createOrReplaceTempView("retentionMonthRange")
        return monthRangeFrame
    }

    def buildHouseholdRetentionBase(spark:SparkSession, personView:String = "personsES", encounterView:String = "encountersES",retentionMonthView:String = "retentionMonthRange") :org.apache.spark.sql.DataFrame = {

        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._    

        // ADD THE PROSPECT RECORDS AT THE BEGINNING OF THE LIFECYCLE CHAIN FOR STANDARD TYPE
        // add the stub records at the end of the lifecycle chain to track end-dates
        val householdRetentionBase = spark.sql(s"""
            with
            cur as (
                select
                household_id,
                cast(from_unixtime(admit_date_ts/1000) as timestamp) as admit_date,
                date_add(cast(from_unixtime(admit_date_ts/1000) as timestamp),-365.25) as admit_date_minus1
                from ${personView} p join ${encounterView} e on e.person_id = p.person_id
                group by household_id,cast(from_unixtime(admit_date_ts/1000) as timestamp),date_add(cast(from_unixtime(admit_date_ts/1000) as timestamp),-365.25)
            ),
            all as (
                select 
                household_id,
                d.startOfMonth,
                0 as isRetained
                from ${personView} p cross join ${retentionMonthView} d 
                group by household_id,d.startOfMonth
            ),
            retained as (
                select cur.household_id,
                trunc(cur.admit_date,'MM') as admit_month,
                1 as isRetained
                from cur join cur as prv
                on cur.household_id = prv.household_id
                and cur.admit_date > prv.admit_date
                and prv.admit_date >= cur.admit_date_minus1
                group by cur.household_id,trunc(cur.admit_date,'MM')
            )
            select
            c.household_id as household,
            row_number() over (partition by c.household_id order by c.startOfMonth) as month_rank,
            c.startOfMonth,
            coalesce(r.isRetained,c.isRetained) as isRetained
            from all c 
            left join retained r on c.household_id = r.household_id and c.startOfMonth = r.admit_month
        """)
        //householdRetentionBase.cache
        householdRetentionBase.createOrReplaceTempView("householdRetentionBase")
        // encounterYearsBetween.show()

        return householdRetentionBase
    } // buildHouseholdRetentionBase

    def buildHouseholdRetentionCollapsed(spark:SparkSession,householdRetentionBaseView:String = "householdRetentionBase") :org.apache.spark.sql.DataFrame = {

        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._    

        val max_admit_month = (new org.joda.time.LocalDate).withDayOfMonth(1)
        val householdRetentionCollapsed = spark.sql(s"""
            with retentionRanked as (
               select 
                household,
                month_rank,
                startOfMonth,
                cast(isRetained as boolean) as isRetained,
                case when lag(isRetained,1,0) over (partition by household order by month_rank) = isRetained and month_rank <> 1 then 1 else 0 end as same_state
                from ${householdRetentionBaseView}
            )
            select
            r.household,
            r.month_rank,
            date_format(r.startOfMonth,'yyyy-MM-dd') as start_date,
            date_format(from_unixtime(unix_timestamp(add_months(coalesce(z.end_of_range,r.startOfMonth),1)) - 1),'yyyy-MM-dd HH:mm:ss') as end_date,
            unix_timestamp(trunc(r.startOfMonth,'MM')) * 1000 as start_date_epoch,
            (unix_timestamp(add_months(coalesce(z.end_of_range,r.startOfMonth),1)) - 1) * 1000 as end_date_epoch,
            r.isRetained
            from retentionRanked r 
            -- all rows at beginning of chain 
            left join (select household, month_rank,startOfMonth from (select household,startOfMonth,month_rank,same_state, lead(same_state) over (partition by household order by month_rank) as next_state from retentionRanked) where same_state = 0 and next_state = 1) c on c.household = r.household and c.month_rank = r.month_rank
            left join (
                select c.household,c.month_rank,c.startOfMonth, min(n.month_rank) as next_month_rank,
                min(case when n.startOfMonth ='${max_admit_month}' then n.startOfMonth else  n.previous_startOfMonth end) as end_of_range
                from
                (select household, month_rank,startOfMonth from (select household,startOfMonth,month_rank,same_state, lead(same_state) over (partition by household order by month_rank) as next_state from retentionRanked) where same_state = 0 and next_state = 1) c
                -- all rows not in same_state
                join (select household,month_rank,same_state
                ,lag(startOfMonth) over (partition by household order by month_rank) as previous_startOfMonth,startOfMonth from retentionRanked) n on n.household = c.household and c.month_rank < n.month_rank 
                and (n.same_state = 0 or n.startOfMonth = '${max_admit_month}')
                group by c.household,c.month_rank,c.startOfMonth
            ) z on r.household = z.household and r.month_rank = z.month_rank
            where r.same_state = 0
        """)
        // householdRetentionCollapsed.cache
        householdRetentionCollapsed.createOrReplaceTempView("householdRetentionCollapsed")

        return householdRetentionCollapsed
    } // buildHouseholdRetentionCollapsed

    def buildHouseholdRetentionFinal(spark:SparkSession,personView:String = "personsES", householdRetentionCollapsedView:String = "householdRetentionCollapsed") :org.apache.spark.sql.DataFrame = {
        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._    

        val householdRetentionFinal = spark.sql(s"""
            with personToHousehold as (
                select
                person_id,
                household_id as household
                from ${personView}
            )
            select
            p.person_id,
            p.household,
            r.start_date,
            r.end_date,
            r.start_date_epoch,
            r.end_date_epoch,
            cast(r.isRetained as Boolean) as retained
            from ${householdRetentionCollapsedView} r join personToHousehold p on p.household = r.household
        """)
        // householdRetentionFinal.cache
        householdRetentionFinal.createOrReplaceTempView("householdRetentionFinal")
        // encounterYearsBetween.show()

        return householdRetentionFinal
    } // buildHouseholdRetentionFinal

    var iterationCount:Int = 1
    var lastCount:Int = 0
    var households:String = ""

    // get the next set of households to process
    // val (householdList,householdCount) = getHouseholdsWithoutRetention(spark,clientCode,esIndexName,esServer,esServerPort,resultLimit)
    //lastCount = householdCount
    //households = householdList

    //while ( lastCount > 0 ) {
        //println("iteration " + iterationCount.toString + " household count " + lastCount.toString)
        //iterationCount = iterationCount + 1

        println("-------- run getPersonSourceData")
        val personsES = getPersonSourceData(spark,clientCode,esIndexName,esServer)
        println("-------- run getEncounterSourceData")
        val encountersES = getEncounterSourceData(spark,clientCode,esIndexName,esServer)
        println("-------- run getRetentionMonthRange")
        val monthRangeFrame = getRetentionMonthRange(spark)
        println("-------- run buildHouseholdRetentionBase")
        val householdRetentionBase = buildHouseholdRetentionBase(spark)
        println("-------- run buildHouseholdRetentionCollapsed")
        val householdRetentionCollapsed = buildHouseholdRetentionCollapsed(spark)
        println("-------- run buildHouseholdRetentionFinal")
        val householdRetentionFinal = buildHouseholdRetentionFinal(spark)
        println("-------- run writeHouseholdRetentionDataToPerson")
        writeHouseholdRetentionDataToPerson(spark,householdRetentionFinal,esIndexName,esServer)
        println("-------- run getEncounterSourceData")

        encountersES.unpersist()
        personsES.unpersist()
        monthRangeFrame.unpersist()
        householdRetentionBase.unpersist()
        householdRetentionCollapsed.unpersist()
        householdRetentionFinal.unpersist()

        // get the next set of households to process
        //val (householdList,householdCount) = getHouseholdsWithoutRetention(spark,clientCode,esIndexName,esServer,esServerPort,resultLimit)
        //lastCount = householdCount
        //households = householdList
    //}

  } // processHouseholdRetentionRetained

  def processHouseholdRetentionNotRetained(spark:SparkSession){
    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._    

    def getPersonSourceData(spark:SparkSession,clientCode:String,esIndexName:String,esServer:String) :org.apache.spark.sql.DataFrame = {
        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._    

        val personSourceQuery = s"""
          {
            "query": {
              "constant_score": {
                "filter": {
                  "bool": {
                    "must": [
                      {"term":{"client_code" : "${clientCode}" }},
                      { "exists": { "field": "household.household_id"} }
                    ],
                    "must_not": [
                      {
                        "nested": {
                          "path": "household_retention_history",
                          "query": {
                            "exists": { "field": "household_retention_history.retained"}
                          }
                        }
                      }
                    ]
                  }
                }
              }
            }
          }
        """
        
        val personSourceQueryOptions = Map(
          "es.read.source.filter" -> "household.household_id",
          "es.read.metadata" -> "true"
        )
        
        val allPersonsWithHouseholdsRDD = EsSpark.esJsonRDD(sc, s"${esIndexName}/person", personSourceQuery, personSourceQueryOptions)
        
        // get jsonMap of response as second item of tuple
        val personsJson = allPersonsWithHouseholdsRDD.map[String](d => d._2)
        personsJson.cache
        
        // read json docs into dataframe
        val allPersonsWithHouseholdsDF = sqlContext.read.json(personsJson)

        // select and rename fields
        val personsES = allPersonsWithHouseholdsDF.select(
          $"_metadata._id".as("person_id"),
          $"household.household_id".as("household_id"))
        personsES.createOrReplaceTempView("personsES")
        personsES.cache
        return personsES

    } // getPersonSourceData

    // return the epoch values that represent the upper and lower bounds for our not retained ranges
    def getRetentionMonthBounds() :(Long, Long) = {
        val current_month = DateUtils.truncate(new Date(),Calendar.MONTH)
        val min_month = DateUtils.addYears(current_month,-4)
        val max_month = DateUtils.addYears(current_month,10)
        
        val min_month_epoch = min_month.getTime()
        val max_month_epoch = max_month.getTime()
        return (min_month_epoch,max_month_epoch)
    } // getRetentionMonthBounds

    def buildHouseholdRetentionFinal(spark:SparkSession,min_month_epoch:Long,max_month_epoch:Long,personView:String = "personsES") :org.apache.spark.sql.DataFrame = {

        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._    

        // BUILD THE FINAL FRAME THAT WILL BE USED TO WRITE NON-RETAINED DATA BACK TO ELASTICSEARCH
        val householdRetentionFinal= spark.sql(s"""
            select
            person_id,
            household_id as household,
            date_format(from_unixtime(${min_month_epoch} / 1000),'yyyy-MM-dd') as start_date,
            date_format(from_unixtime(${max_month_epoch} / 1000),'yyyy-MM-dd HH:mm:ss') as end_date,
            ${min_month_epoch} as start_date_epoch,
            ${max_month_epoch} as end_date_epoch,
            false as retained
            from ${personView}
        """)
        // householdRetentionFinal.cache
        householdRetentionFinal.createOrReplaceTempView("householdRetentionFinal")

        return householdRetentionFinal
    }
 
    case class householdRetentionNotReadyException(private val message: String = "", private val cause: Throwable = None.orNull) extends Exception(message, cause) 

    val (householdList,householdCount) = getHouseholdsWithoutRetention(spark,clientCode,esIndexName,esServer,esServerPort)
    if (householdCount > 0) {
        throw new householdRetentionNotReadyException("households eligible for retention are available and not processed.  Household retention processing step 1 needs to be run to completion before running step 2.")
    }
    val (min_month_epoch,max_month_epoch) = getRetentionMonthBounds()
    val personsES = getPersonSourceData(spark,clientCode,esIndexName,esServer)
    val householdRetentionFinal = buildHouseholdRetentionFinal(spark,min_month_epoch,max_month_epoch)
    writeHouseholdRetentionDataToPerson(spark,householdRetentionFinal,esIndexName,esServer)



  } // processHouseholdRetentionRetained


}
