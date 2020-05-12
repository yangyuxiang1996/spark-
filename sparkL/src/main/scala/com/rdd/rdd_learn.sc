import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("spark learning")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

val sc = spark.sparkContext

val rdd = sc.makeRDD(Seq(("数学", 95), ("语文", 84), ("英语", 95), ("数学", 90), ("语文", 90)))

// transform操作
val rddMap = rdd.map(row => (row._1, 1))
println("[map]: " + rddMap.collect.mkString(","))

val rddFilter = rdd.filter(row => row._1.equals("数学"))
println("[filter]: " + rddFilter.collect.mkString(","))

val rdd1 = spark.read.textFile("/Users/mac/Desktop/spark/word_count.txt")
rdd1.collect
val rddFlatMap = rdd1.flatMap(_.split(" "))
println("[flatMap]: " + rddFlatMap.collect.mkString(","))
rddFlatMap.collect

val rddGroupBy = rdd.groupBy(r => r._1)
println("[groupBy]: " + rddGroupBy.collect.mkString(","))

val rdd2 = sc.parallelize(List(4,24,5,6,2,1), 1)
val rdd2Group = rdd2.groupBy(r => {if (r % 2 == 0) "even" else "odd"})
println("[groupBy]: " + rdd2Group.collect.mkString(","))

val rdd3 = sc.makeRDD(Seq(("数学", 65), ("语文", 43), ("地理", 45)))
val rddCogroup = rdd.cogroup(rdd3)
println("[cogroup]: " + rddCogroup.collect.mkString(","))

val rddGroupByKey = rdd.groupByKey()
println("[rddGroupByKey]: " + rddGroupByKey.collect.mkString(","))

val rddReduceByKey = rdd.reduceByKey(_ + _)
println("[rddReduceByKey]: " + rddReduceByKey.collect.mkString(","))

val rddReduceByKey1 = rdd.reduceByKey((a, b) => a + b)
println("[rddReduceByKey]: " + rddReduceByKey1.collect.mkString(","))

val rddAverage = rdd.map(row => (row._1, (row._2, 1)))
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .mapValues(v => v._1 / v._2.toDouble )
println("[rddAverage]: " + rddAverage.collect.mkString(","))

println("[rdd.partition.size]: " + rdd.partitions.length)
val rdd4 = sc.makeRDD(Seq(("数学", 95), ("语文", 84), ("英语", 95), ("数学", 90), ("语文", 90)), numSlices = 3)
println("[rdd.partition.size]: " + rdd4.partitions.length)
rdd4.glom().collect()

val rddCombine = rdd.combineByKey(
  v => (v, 1),
  (buff: (Int, Int), v) => (buff._1 + v, buff._2 + 1),
  (a: (Int, Int), b: (Int, Int)) => (a._1 + b._1, a._2 + b._2)
)
println("[rddCombine]: " + rddCombine.collect.mkString(","))

val rddAverage1 = rddCombine.map { case (key, value) => (key, value._1 / value._2.toDouble) }
println("[rddAverage1]: " + rddAverage1.collect.mkString(","))

var rdd5 = sc.makeRDD(Array(("A",1),("A",2),("B",1),("B",2),("C",1)), 1)
rdd5.glom().collect
rdd5.combineByKey(
  (v: Int) => v + "_",
  (c: String, v: Int) => c + "@" + v,
  (c1: String, c2: String) => c1 + "$" + c2
).collect
println("[rdd.partition.size]: " + rdd5.partitions.length)

rdd5.combineByKey(
  (v: Int) => List(v),
  (c: List[Int], v: Int) => c :+ v,
  (c1: List[Int], c2: List[Int]) => c1 ::: c2
).collect

rdd5.foldByKey(0)(_ + _).collect
rdd5.foldByKey(1)(_ * _).collect

val rdd6 = sc.makeRDD(1 to 10, 3)
println("[rdd.partition.size]: " + rdd6.partitions.length)
rdd6.glom().collect

rdd2.fold(0)(_ + _)







