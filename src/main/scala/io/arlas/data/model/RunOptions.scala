package io.arlas.data.model

import java.time.{ZonedDateTime}

case class RunOptions (
                      source: String,
                      target: String,
                      start: Option[ZonedDateTime],
                      stop: Option[ZonedDateTime],
                      warmingPeriod : Option[Long],
                      endingPeriod : Option[Long]
                      )