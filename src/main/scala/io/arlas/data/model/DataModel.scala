package io.arlas.data.model

case class DataModel(
                    idColumn: String = "id",
                    timestampColumn: String = "timestamp",
                    timeFormat: String = "yyyy-MM-dd'T'HH:mm:ssZ",
                    latColumn: String = "lat",
                    lonColumn: String = "lon",
                    dynamicFields: Array[String] = Array("lat","lon"),
                    sequenceGap: Int = 3600, //in seconds
                    timeSampling: Long = 15 //in seconds
                    )
