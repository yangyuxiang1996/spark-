import org.apache.spark.sql.SparkSession

val spark = SparkSession
  .builder()
  .appName("spark learning")
  .master("local[*]")
  .getOrCreate()

import spark.implicits._

val sc = spark.sparkContext

val a = sc.parallelize(1 to 20, 2)

def mapTerFunc(a: Int): Int = {
  a * 3
}

val mapResult = a.map(mapTerFunc)
println(mapResult.collect.mkString(","))

List(1,2).::(1)

List(2,3).::(1)

var b = List[Int]()
b.::= (1)
b
b.::= (2)
b
b::= (3)
b+:= (4)
b
b:+=(5)
b

def terFunc(iter: Iterator[Int]): Iterator[Int] = {
  var res = List[Int]()
  while (iter.hasNext) {
    val next = iter.next
    res::= (next)
  }
  res.iterator
}

val result = a.mapPartitions(terFunc)
println(result.collect().mkString(","))

def terFunc1(iter: Iterator[Int]): Iterator[Int] = {
  var res = List[Int]()
  while (iter.hasNext) {
    val next = iter.next
    res::= (next*3)
  }
  res.iterator
}

val result1 = a.mapPartitions(terFunc1)
println(result1.collect().mkString(","))

//上述做法会在mapPartitions过程中在内存中定义一个数组并将缓存所有的数据，
//加入数据集比较大，内存不足，会导致内存溢出，任务失败。
//下面的写法无需缓存数据，自定义一个迭代器，较为高校

class CustomIterator(iter: Iterator[Int]) extends Iterator[Int] {
  override def hasNext: Boolean = iter.hasNext

  override def next(): Int = {
    val value = iter.next
    value * 3
  }
}

val result2 = a.mapPartitions(v => new CustomIterator(v))
println(result2.collect.mkString(","))

val result3 = a.mapPartitionsWithIndex { (i, item) =>
  val partitionsMap = scala.collection.mutable.Map[Int, List[Int]]()
  partitionsMap(i) = new CustomIterator(item).toList
  partitionsMap.iterator
}
result3.collect

println(result3.collect.mkString(","))