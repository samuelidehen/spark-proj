package ca.mcit.spark
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCore extends App {
  val sparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("Assignment 2")

  val sc = new SparkContext(sparkConf)

  val tripsRdd:RDD[Trip] =
    sc.textFile("/user/winter2020/samuel/stm/trips.txt")
        .filter(!_.contains("route_id"))
        .map(Trip.fromCsv)

  val routesRdd:RDD[Route] =
    sc.textFile("/user/winter2020/samuel/stm/routes.txt")
      .filter(!_.contains("route_id"))
      .map(Route.fromCsv)

  val calendarRdd:RDD[Calendar] =
    sc.textFile("/user/winter2020/samuel/stm/calendar.txt")
      .filter(!_.contains("service_id"))
      .map(Calendar.fromCsv)

  val x:RDD[(String,Trip)] =tripsRdd.keyBy(_.service_id)
  val y:RDD[(String,Calendar)]=  calendarRdd.keyBy(_.service_id)
  val z:RDD[(Int, Route)] =  routesRdd.keyBy(_.route_id)

  val a = x.join(y).keyBy(x=>x._2._1.route_id)
  val enrichedTrip = a.join(z).map{
    case((route_id,((service_id,(trip,calendar)),route))) => EnrichedTrip.toCsv(trip,calendar,route)
  }

  enrichedTrip.saveAsTextFile("/user/winter2020/samuel/assignment2/enriched.csv")
  sc.stop()
}

