package ca.mcit.spark

case class Trip(
                 route_id: Int,
                 service_id: String,
                 trip_id: String,
                 trip_headsign: String,
                 direction_id: Int,
                 shape_id: Int,
                 wheelchair_accessible: Int,
                 note_fr: Option[String],
                 note_en: Option[String]
               )
object Trip{
  def fromCsv(trip:String):Trip = {
    val t =trip.split(",",-1)
    Trip(t(0)toInt, t(1), t(2), t(3), t(4).toInt,t(5).toInt,t(6).toInt,
      if (t(7).isEmpty) None else Some(t(7)),
      if (t(8).isEmpty) None else Some(t(8)))
  }

}

object EnrichedTrip{
  def toCsv(trip :Trip, calendar:Calendar, route:Route): String =
    s"${trip.route_id},${trip.trip_headsign},${calendar.service_id},${calendar.start_date},${route.route_long_name},${route.route_color}"
}
case class Route(
                  route_id: Int,
                  agency_id: String,
                  route_short_name: String,
                  route_long_name: String,
                  route_type: String,
                  route_url: String,
                  route_color: String,
                  route_text_color: String
                )
object Route{
  def fromCsv(route:String):Route = {
    val r =route.split(",",-1)
    Route(r(0).toInt, r(1), r(2), r(3), r(4),r(5),r(6),r(7))
  }
}
case class Calendar(service_id:String,
                    monday:Int,
                    tuesday:Int,
                    wednesday:Int,
                    thursday:Int,
                    friday:Int,
                    saturday:Int,
                    sunday:Int,
                    start_date:Int,
                    end_date:Int
                   )

object Calendar{
  def fromCsv(calendar:String):Calendar = {
    val p =calendar.split(",",-1)
    Calendar(p(0), p(1).toInt, p(2).toInt, p(3).toInt, p(4).toInt, p(5).toInt, p(6).toInt, p(7).toInt, p(8).toInt,p(9).toInt)
  }
}
