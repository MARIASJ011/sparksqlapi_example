package edu.vcu.sparksqlapi_example

object GeoClean {
  def cleanGeography(geo: String): String = {
    // first remove the geography's numerical code from the record using a regular expression
    val geoNoNum = geo.replaceFirst("(\\d+\\.\\s)", "").toLowerCase()
    // then, remove the word "county" we are doing this to enable the crash data to be joined to the OD data
    geoNoNum.replaceAll("\\s(county)", "")
  } 
}
