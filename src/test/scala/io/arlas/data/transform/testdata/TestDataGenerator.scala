package io.arlas.data.transform.testdata

import org.apache.spark.sql.DataFrame

trait TestDataGenerator {
  def get(): DataFrame
}
