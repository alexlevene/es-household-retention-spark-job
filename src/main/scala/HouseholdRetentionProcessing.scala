
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

import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.apache.spark.sql.Row

import org.apache.spark.sql.SparkSession


// sample local command to run locally:
//make run-local ARGS="DEMO elasticsearch.exp-dev.io exp_rjj_1_2 10000"
//make run-local ARGS="DEMO 100.70.102.71 exp_rjj_1_2 10000"

object HouseholdRetentionProcessing {

  var esServer:String = "exp-elasticsearch.default.svc.cluster.local"
  var esWriteOperation:String = "upsert"
  var esBatchSize:String = "20mb"
  var esServerPort:Int = 9200
  var esIndexName:String = "exp_v1_0_1"
  var clientCode:String = "DEMO"
  var batchSize:Int = 10000
  var defaultLogLevel = "INFO"

  case class date_range (
      gte: Long,
      lte: Long
      )
  case class household_retention_history (
      date_range: date_range,
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
      esBatchSize = scala.util.Properties.envOrElse("ES_WRITE_BATCH_SIZE", esBatchSize)
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
    println(s"batchSize: ${batchSize}")
    println(s"esBatchSize: ${esBatchSize}")


    

    // Create a SparkSession
    val spark = SparkSession
      .builder()
      .appName("HouseholdRetentionProcessing")
      .config("es.index.auto.create", false)
      .config("es.nodes", scala.util.Properties.envOrElse("ES_HOST", esServer))
      .config("es.nodes.wan.only", false)
      .config("es.write.operation", esWriteOperation)
      .config("es.batch.size.bytes", esBatchSize)
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
                      "child_type": "encounter",
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
      val request_url = "http://" + esHost + ":" + esPort + endPoint
      
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

      var esconf = Map("es.nodes" -> esServer, "es.write.operation" -> esWriteOperation, "es.nodes.wan.only" -> "true","es.index.auto.create" -> "false" ,"es.batch.size.bytes" -> "20mb")


    def getHouseholdRetentionHistory(rows: Iterable[Row]): Array[household_retention_history] = {
      rows.map(a =>
        household_retention_history(
          date_range(a.getAs[Long]("start_date_epoch"), a.getAs[Long]("end_date_epoch")),
          a.getAs[Boolean]("isRetained")
        )
      ).toArray
    }
    val updates_full = householdRetentionFinal
      .rdd
      .groupBy(z => z.getAs[String]("mastered_person_id") + "|" + z.getAs[String]("household") + "|" + z.getAs[String]("income_min") + "|" + z.getAs[String]("income_max"))
      .map(e => (
            e._1.split("\\|")(0),
            Map(
                "household_retention_history" -> getHouseholdRetentionHistory(e._2)
            )
         )
      )

      // write elasticsearch data back to the index
      updates_full.saveToEsWithMeta(s"${esIndexName}/person", esconf)
  } // writeHouseholdRetentionDataToPerson

  def processHouseholdRetentionRetained(spark:SparkSession, resultLimit:Int = 10000){

    val sc = spark.sparkContext
    val sqlContext = spark.sqlContext
    import sqlContext.implicits._    

    def getEncounterSourceData(spark:SparkSession,clientCode:String,esIndexName:String,esServer:String,householdList:String) :org.apache.spark.sql.DataFrame = {

        val encounterSourceQuery = s"""
          {
            "_source": ["encounter_key","mastered_person_id","admit_date"],
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
                                {"terms": {"household.household_id": [
          ${householdList}
                                  ]
                                }}
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
        //  "es.read.field.include" -> "encounter_key,parent,admit_date"
          "es.read.field.exclude" -> "recordId,admit_age,service_category,facility,patient_lifecycle,admit_source,admit_type,client_code,client_name,discharge_date,encounter_type,financial_class,msdrg,service_sub_category,diagnosis,cpt,procedure,campaigns,total_charges,total_amount_received,expected_reimbursement,direct_costs,actual_contribution_margin,expected_contribution_margin,recency_frequency",
            "es.read.metadata" -> "true", "es.nodes" -> esServer, "es.write.operation" -> "upsert", "es.nodes.wan.only" -> "true","es.index.auto.create" -> "false"
          )
        
        val encountersES = sqlContext.esDF(s"${esIndexName}/encounter", encounterSourceQuery, encounterSourceQueryOptions)
        encountersES.cache
        encountersES.createOrReplaceTempView("encountersES")
        return encountersES
    } // getEncounterSourceData

    def getPersonSourceData(spark:SparkSession,clientCode:String,esIndexName:String,esServer:String,householdList:String) :org.apache.spark.sql.DataFrame = {

        val sc = spark.sparkContext
        val sqlContext = spark.sqlContext
        import sqlContext.implicits._    

        val personSourceQuery = s"""
            {
              "_source": ["_recordId","household.household_id"],
              "query": {
                "constant_score": {
                  "filter": {
                    "bool": {
                      "must": [
                        {"term":{"client_code" : "${clientCode}" }},
                        {"terms": {"household.household_id": [
            ${householdList}
                          ]
                        }}
                      ]
                    }
                  }
                }
              }
            }
        """
        // limit the fields that are included
        val personSourceQueryOptions = Map(
        //  "es.read.field.include" -> "_recordId,household.household_id"
          "es.read.field.exclude" -> "address,birth_date,children_present,client_code,client_name,communication,email,ethnicity,first_name,gender,language,last_name,marital_status,middle_initial,middle_name,mobile_phone,personCount,payor_category,payor_category_confidence,patientYear,prefix,race,religion,size,suffix,campaigns,recency_frequency,perceptual_profile,chui,pdi,patient_lifecycle_history,deceased_date,household_retention_history,last_update_by_batch",
            "es.read.metadata" -> "true", "es.nodes" -> esServer, "es.write.operation" -> "upsert", "es.nodes.wan.only" -> "true","es.index.auto.create" -> "false"
          )
        
        val personsES = sqlContext.esDF(s"${esIndexName}/person", personSourceQuery, personSourceQueryOptions)
        personsES.cache
        personsES.createOrReplaceTempView("personsES")
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
                household.household_id as household,
                e.admit_date,
                date_add(e.admit_date,-365.25) as admit_date_minus1
                from ${personView} p join ${encounterView} e on e._metadata._parent = p._metadata._id
                group by household.household_id,e.admit_date,date_add(e.admit_date,-365.25)
            ),
            all as (
                select 
                p.household.household_id as household,
                d.startOfMonth,
                0 as isRetained
                from ${personView} p cross join ${retentionMonthView} d 
                group by p.household.household_id,d.startOfMonth
            ),
            retained as (
                select cur.household,
                trunc(cur.admit_date,'MM') as admit_month,
                1 as isRetained
                from cur join cur as prv
                on cur.household = prv.household
                and cur.admit_date > prv.admit_date
                and prv.admit_date >= cur.admit_date_minus1
                group by cur.household,trunc(cur.admit_date,'MM')
            )
            select
            c.household,
            row_number() over (partition by c.household order by c.startOfMonth) as month_rank,
            c.startOfMonth,
            coalesce(r.isRetained,c.isRetained) as isRetained
            from all c 
            left join retained r on c.household = r.household and c.startOfMonth = r.admit_month
        """)
        householdRetentionBase.cache
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
u               select 
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
        householdRetentionCollapsed.cache
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
                _metadata._id as mastered_person_id,
                cast(household.income.range.minimum as Long) as income_min,
                cast(household.income.range.maximum as Long) as income_max,
                household.household_id as household
                from ${personView}
            )
            select
            p.mastered_person_id,
            p.income_min,
            p.income_max,
            p.household,
            r.start_date,
            r.end_date,
            r.start_date_epoch,
            r.end_date_epoch,
            cast(r.isRetained as Boolean) as isRetained
            from ${householdRetentionCollapsedView} r join personToHousehold p on p.household = r.household
        """)
        householdRetentionFinal.cache
        householdRetentionFinal.createOrReplaceTempView("householdRetentionFinal")
        // encounterYearsBetween.show()

        return householdRetentionFinal
    } // buildHouseholdRetentionFinal

    var iterationCount:Int = 1
    var lastCount:Int = 0
    var households:String = ""

    // get the next set of households to process
    val (householdList,householdCount) = getHouseholdsWithoutRetention(spark,clientCode,esIndexName,esServer,esServerPort,resultLimit)
    lastCount = householdCount
    households = householdList

    while ( lastCount > 0 ) {
        println("iteration " + iterationCount.toString + " household count " + lastCount.toString)
        iterationCount = iterationCount + 1

        val encountersES = getEncounterSourceData(spark,clientCode,esIndexName,esServer,households)
        val personsES = getPersonSourceData(spark,clientCode,esIndexName,esServer,households)
        val monthRangeFrame = getRetentionMonthRange(spark)
        val householdRetentionBase = buildHouseholdRetentionBase(spark)
        val householdRetentionCollapsed = buildHouseholdRetentionCollapsed(spark)
        val householdRetentionFinal = buildHouseholdRetentionFinal(spark)
        writeHouseholdRetentionDataToPerson(spark,householdRetentionFinal,esIndexName,esServer)

        encountersES.unpersist()
        personsES.unpersist()
        monthRangeFrame.unpersist()
        householdRetentionBase.unpersist()
        householdRetentionCollapsed.unpersist()
        householdRetentionFinal.unpersist()

        // get the next set of households to process
        val (householdList,householdCount) = getHouseholdsWithoutRetention(spark,clientCode,esIndexName,esServer,esServerPort,resultLimit)
        lastCount = householdCount
        households = householdList
    }

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
            "_source": ["_recordId","household"],
            "query": {
              "constant_score": {
                "filter": {
                  "bool": {
                    "must": [
                      {"term":{"client_code" : "${clientCode}" }},
                      {"exists": { "field": "household.household_id"} }
                    ],
                    "must_not": [
                      {
                        "nested": {
                          "path": "household.household_retention_history",
                          "query": {
                            "exists": { "field": "household.household_retention_history.retained"}
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
        val personSourceQueryOptions = Map(
        //  "es.read.field.include" -> "_recordId,household"
          "es.read.field.exclude" -> "address,birth_date,children_present,client_code,client_name,communication,email,ethnicity,first_name,gender,language,last_name,marital_status,middle_initial,middle_name,mobile_phone,payor_category,payor_category_confidence,prefix,race,religion,suffix,campaigns,recency_frequency,perceptual_profile,chui,pdi,patient_lifecycle_history,deceased_date",
            "es.read.metadata" -> "true", "es.nodes" -> esServer, "es.write.operation" -> "upsert", "es.nodes.wan.only" -> "true","es.index.auto.create" -> "false"
          )
        
        val personsES = sqlContext.esDF(s"${esIndexName}/person", personSourceQuery, personSourceQueryOptions)
        personsES.cache
        personsES.createOrReplaceTempView("personsES")
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
            _metadata._id as mastered_person_id,
            cast(household.household_income_range.minimum as Long) as income_min,
            cast(household.household_income_range.maximum as Long) as income_max,
            household.household_id as household,
            date_format(from_unixtime(${min_month_epoch} / 1000),'yyyy-MM-dd') as start_date,
            date_format(from_unixtime(${max_month_epoch} / 1000),'yyyy-MM-dd HH:mm:ss') as end_date,
            ${min_month_epoch} as start_date_epoch,
            ${max_month_epoch} as end_date_epoch,
            false as isRetained
            from ${personView}
        """)
        householdRetentionFinal.cache
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
