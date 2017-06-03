/*
 * Incremental calculation for variance.
 *
 */
package org.apache.spark.sql.catalyst.util

object commonMath {

  def CNDF(x: Double): Double = {
    var mX = x
    val neg = if (mX < 0d) 1
    else 0
    if (neg == 1) mX = mX * -1d

    val k = 1d / (1d + 0.2316419 * mX)

    var y = ((((1.330274429 * k - 1.821255978) * k + 1.781477937) * k - 0.356563782)
      * k + 0.319381530) * k
    y = 1.0 - 0.398942280401 * Math.exp(-0.5 * mX * mX) * y

    (1d - neg) * y + neg * (1d - y)
  }

  def main(args: Array[String]): Unit = {
    implicit val arrayToSamples = (values: Array[Double]) => NumberSamples(values)

    // 历史的一批 samples(长度随意)
    val historicalSamples = Array(1.5d, 3.4d, 7.8d, 11.6d)
    // 新增的一批 samples(长度随意)
    val deltaSamples = Array(9.4d, 4.2d, 35.6d, 77.9d)

    // 增量方差
    var deltaVar =
      historicalSamples.measures.appendDelta(deltaSamples.measures).variance

  }

  // 根据 errorBound, samplesCount 和 T 计算置信度 P,
  // T 需要在对应的聚合函数里增量求得.
  def calcConfidence(errorBound: Double,
                     samplesCount: Long,
                     T_Denominator: Double): Double = {
    2 * CNDF((errorBound * math.sqrt(samplesCount)) / T_Denominator) - 1
  }
}

case class DeltaVarianceMeasures(n: Int, sum: Double, variance: Double) {
  def avg = sum / n

  def appendDelta(delta: DeltaVarianceMeasures): DeltaVarianceMeasures = {
    val newN = this.n + delta.n
    val newSum = this.sum + delta.sum
    val newAvg = newSum / newN

    def partial(m: DeltaVarianceMeasures): Double = {
      val deltaAvg = newAvg - m.avg
      m.n * (m.variance + deltaAvg * deltaAvg)
    }

    val newVariance = (partial(this) + partial(delta)) / newN

    DeltaVarianceMeasures(newN, newSum, newVariance)
  }
}

case class NumberSamples(values: Seq[Double]) {
  def measures: DeltaVarianceMeasures = {
    if (values == null || values.isEmpty) {
      DeltaVarianceMeasures(0, 0d, 0d)
    }
    else {
      DeltaVarianceMeasures(values.length, values.sum, variance)
    }
  }

  private def variance: Double = {
    val n = values.length
    val avg = values.sum / n
    values.foldLeft(0d) { case (sum, sample) =>
      sum + (sample - avg) * (sample - avg)
    } / n
  }
}