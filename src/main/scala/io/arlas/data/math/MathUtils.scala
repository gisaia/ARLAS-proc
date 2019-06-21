package io.arlas.data.math

object MathUtils {

  def ~=(x: Double, y: Double, precision: Double) = {
    if ((x - y).abs < precision) true else false
  }

  def distance(x1: Double, x2: Double, y1: Double, y2: Double): Double = {
    Math.sqrt(
      Math.pow(x2 - x1, 2d)
      + Math.pow(y2 - y1, 2d))
  }

}
