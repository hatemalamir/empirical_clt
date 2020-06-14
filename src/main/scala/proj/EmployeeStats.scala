package proj

import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable

object EmployeeStats extends App {

  override def main(args: Array[String]) {

    if (args.length < 4 || (args(0) != "local" && args(0) != "cluster") || !args(1).forall(_.isDigit)) {
      System.err.println(
        s"""
           |Usage: EmployeeStats <mode> <iterations> <input> <output>
           |  <mode> execution mode, local/cluster
           |  <iterations> number of sampling iterations
           |  <input> input file path
           |  <output> output file path
        """.stripMargin)
      System.exit(1)
    }

    val mstr = if (args(0) == "local") "local[*]" else "spark://quickstart.cloudera:8030";
    val numIterations = args(1).toInt
    val inputPath = args(2)

    val SEED = 1990
    val SAMP_FRAC_MIN = 0.1
    val SAMP_FRAC_STEP = 0.1
    val SAMP_FRAC_MAX = 0.9
    val DEPT_COUNT = 10
    val SAMP_SIZE_MIN = 5

    val ss = SparkSession.builder.appName("Employee Salaries - Spark").master(mstr).getOrCreate()
    val csv = ss.sparkContext.textFile(inputPath)

    ss.sparkContext.setLogLevel("WARN")

    val headerAndData = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndData.first
    val data = headerAndData.filter(_ (0) != header(0))

    import ss.implicits._

    val empDS = data.map(record => Employee(record(0), record(1), record(2), record(3), record(4), record(5),
      if (!record(6).isEmpty) record(6).toInt else 0,
      if (!record(7).isEmpty) record(7).toDouble else 0,
      if (record.length == 9 && !record(8).isEmpty) record(8).toDouble else 0)).toDS
    val annualEmps = empDS.filter($"annualSalary" > 0).cache()

    import org.apache.spark.sql.functions._
    val departments = annualEmps.groupBy("department").agg(count("department"))
      .sort($"count(department)".desc).limit(DEPT_COUNT).select("department").map(d => d.getString(0)).collect().toList

    val popAvg = annualEmps.filter($"department".isin(departments: _*)).groupBy("department")
      .agg(avg("annualSalary")).sort($"avg(annualSalary)".desc)
    val popStddev = annualEmps.filter($"department".isin(departments: _*)).groupBy("department")
      .agg(stddev_pop("annualSalary")).sort($"stddev_pop(annualSalary)".desc)

    println("Population Mean and Standard Deviation: ")
    popAvg.show()
    popStddev.show()

    val popAvgCache = popAvg.collect.map(rec => Map(popAvg.columns.zip(rec.toSeq): _*))
      .map(m => (m.getOrElse("department", null), m.getOrElse("avg(annualSalary)", null))).toMap

    val popStddevCache = popStddev.collect.map(rec => Map(popStddev.columns.zip(rec.toSeq): _*))
      .map(m => (m.getOrElse("department", null), m.getOrElse("stddev_pop(annualSalary)", null))).toMap

    val sampAvgStats = collection.mutable.Map[String, java.util.Map[Double, Double]]()
    val sampStddevStats = collection.mutable.Map[String, java.util.Map[Double, Double]]()

    println("\nSample Stats by department: ")
    for (dept <- departments) {
      val deptEmps = annualEmps.filter($"department" === dept).select("annualSalary")

      val avgStats = collection.mutable.Map[Double, Double]()
      val stddevStats = collection.mutable.Map[Double, Double]()
      var sampFraq = SAMP_FRAC_MIN
      while (sampFraq <= SAMP_FRAC_MAX) {
        val deptEmpsFraq = deptEmps.sample(false, sampFraq, SEED)
        val sampSize = deptEmpsFraq.count()
        if (sampSize >= SAMP_SIZE_MIN) {
          var totAvg = 0.0
          var totStddev = 0.0
          var i = numIterations
          while (i > 0) {
            val sample = deptEmpsFraq.sample(true, 1.0, 1990);
            totAvg = sample.agg(avg("annualSalary").cast("double")).first.getDouble(0) + totAvg
            totStddev = sample.agg(stddev("annualSalary").cast("double")).first.getDouble(0) + totStddev
            i = i - 1;
          }
          val sampAvg = totAvg / numIterations
          val sampStddev = totStddev / numIterations
          avgStats(sampFraq) = Math.abs(popAvgCache(dept).toString.toDouble - sampAvg) * 100 / popAvgCache(dept).toString.toDouble
          stddevStats(sampFraq) = Math.abs(popStddevCache(dept).toString.toDouble - sampStddev) * 100 / popStddevCache(dept).toString.toDouble
          println(dept + " -> Sample Fraction: " + sampFraq + ", Sample Size: " + sampSize + ", Avg: " + sampAvg + ", Std Dev: " + sampStddev)
        }
        sampFraq = sampFraq + SAMP_FRAC_STEP
      }
      sampAvgStats(dept) = ListMap(avgStats.toSeq.sortBy(_._1): _*).asJava
      sampStddevStats(dept) = ListMap(stddevStats.toSeq.sortBy(_._1): _*).asJava
    }

    import scala.collection.JavaConversions._

    val avgErrList = mutable.MutableList[String]()
    sampAvgStats.foreach(o => {
      val dept = o._1
      o._2.foreach(kv => avgErrList += dept + ", " + kv._1 + ", " + kv._2)
    })

    ss.sparkContext.parallelize(avgErrList).saveAsTextFile(args(3) + "/emp_stats_avg")

    val stdErrList = mutable.MutableList[String]()
    sampStddevStats.foreach(o => {
      val dept = o._1
      o._2.foreach(kv => stdErrList += dept + ", " + kv._1 + ", " + kv._2)
    })

    ss.sparkContext.parallelize(stdErrList).saveAsTextFile(args(3) + "/emp_stats_std")

    ss.stop()
  }
}
