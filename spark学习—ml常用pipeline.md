# spark mlib 中常用的pipeline总结

[TOC]

## StringIndexer

[StringIndexer](http://spark.apache.org/docs/latest/ml-features.html#stringindexer)将标签的字符串列编码为标签索引的列。 StringIndexer可以编码多个列。索引位于[0，numLabels）中，并支持四个排序选项：“ frequencyDesc”：按标签频率的降序（最频繁的标签分配为0），“ frequencyAsc”：按标签频率的升序（最不频繁的标签分配为0） ，“ alphabetDesc”：字母降序，“ alphabetAsc”：字母升序（默认=“ frequencyDesc”）。注意，在“ frequencyDesc” /“ frequencyAsc”下，如果频率相等，则字符串将按字母进一步排序。

如果用户选择保留看不见的标签，则会将它们放在索引numLabels处。如果输入列为数字，则将其强制转换为字符串并为字符串值编制索引。当下游管道组件（例如Estimator或Transformer）使用此字符串索引标签时，必须将组件的输入列设置为此字符串索引列名称。在许多情况下，可以使用setInputCol设置输入列。

```scala
// $example on$
val df = spark.createDataFrame(
  Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
).toDF("id", "category")

val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex")
  .setHandleInvalid("keep") // 保留unseen labels, 选择skip可以跳过

val indexed = indexer.fit(df).transform(df)
indexed.show()
// $example off$

val df1 = spark.createDataFrame(
  Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "d"), (5, "e"))
).toDF("id", "category")
indexer.fit(df).transform(df1).show()
```

输出结果：

```
+---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          2.0|
|  2|       c|          1.0|
|  3|       a|          0.0|
|  4|       a|          0.0|
|  5|       c|          1.0|
+---+--------+-------------+

+---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          2.0|
|  2|       c|          1.0|
|  3|       a|          0.0|
|  4|       d|          3.0|
|  5|       e|          3.0|
+---+--------+-------------+
```



## [Tokenizer](http://spark.apache.org/docs/latest/ml-features.html#tokenizer)

Tokenization是获取文本（例如句子）并将其分解为单个术语（通常是单词）的过程。一个简单的Tokenizer类提供了此功能。下面的示例显示了如何将句子分成单词序列。

RegexTokenizer允许基于正则表达式（regex）匹配进行更高级的标记化。默认情况下，参数“ pattern”（正则表达式，默认值：“ \\\\ s +”）用作分隔输入文本的定界符。或者，用户可以将参数“ gap”设置为false，以表示正则表达式“ pattern”表示“令牌”，而不是拆分间隙，并找到所有匹配的出现作为标记化结果。

```scala
// $example on$
val sentenceDataFrame = spark.createDataFrame(Seq(
  (0, "Hi I heard about Spark"),
  (1, "I wish Java could use case classes"),
  (2, "Logistic,regression,models,are,neat")
)).toDF("id", "sentence")

val tokenizer = new Tokenizer().setInputCol("sentence").setOutputCol("words") // 默认 //s+
val regexTokenizer = new RegexTokenizer()
  .setInputCol("sentence")
  .setOutputCol("words")
  .setPattern("\\W")

val countTokens = udf { (words: Seq[String]) => words.length }

val tokenized = tokenizer.transform(sentenceDataFrame)
tokenized.select("sentence", "words")
  .withColumn("tokens", countTokens(col("words"))).show(false)

val regexTokenized = regexTokenizer.transform(sentenceDataFrame)
regexTokenized.select("sentence", "words")
  .withColumn("tokens", countTokens(col("words"))).show(false)
```

输出结果：

```
+-----------------------------------+------------------------------------------+------+
|sentence                           |words                                     |tokens|
+-----------------------------------+------------------------------------------+------+
|Hi I heard about Spark             |[hi, i, heard, about, spark]              |5     |
|I wish Java could use case classes |[i, wish, java, could, use, case, classes]|7     |
|Logistic,regression,models,are,neat|[logistic,regression,models,are,neat]     |1     |
+-----------------------------------+------------------------------------------+------+

+-----------------------------------+------------------------------------------+------+
|sentence                           |words                                     |tokens|
+-----------------------------------+------------------------------------------+------+
|Hi I heard about Spark             |[hi, i, heard, about, spark]              |5     |
|I wish Java could use case classes |[i, wish, java, could, use, case, classes]|7     |
|Logistic,regression,models,are,neat|[logistic, regression, models, are, neat] |5     |
+-----------------------------------+------------------------------------------+------+
```

> \s：用于匹配单个空格符，包括tab键和换行符； 
> \S：用于匹配除单个空格符之外的所有字符； 
> \d：用于匹配从0到9的数字； 
> \w：用于匹配字母，数字或下划线字符； 
> \W：用于匹配所有与\w不匹配的字符； 
> . ：用于匹配除换行符之外的所有字符。



## [CountVectorizer](http://spark.apache.org/docs/latest/ml-features.html#countvectorizer)

CountVectorizer和CountVectorizerModel旨在帮助将文本文档的集合转换为令牌计数的向量。当先验字典不可用时，CountVectorizer可用作Estimator以提取词汇表并生成CountVectorizerModel。该模型为词汇表上的文档生成稀疏表示，然后可以将其传递给其他算法，例如LDA。

在拟合过程中，CountVectorizer将选择整个语料库中按词频排列的前vocabSize词。可选参数minDF还通过指定一个词条必须出现在词汇表中的最小数量（或小数，如果小于1.0）来影响拟合过程。另一个可选的二进制切换参数控制输出向量。如果将其设置为true，则所有非零计数都将设置为1。这对于模拟二进制而不是整数计数的离散概率模型特别有用。

```scala
// $example on$
val df = spark.createDataFrame(Seq(
  (0, Array("a", "b", "c")),
  (1, Array("a", "b", "b", "c", "a"))
)).toDF("id", "words")

// fit a CountVectorizerModel from the corpus
val cvModel: CountVectorizerModel = new CountVectorizer()
  .setInputCol("words")
  .setOutputCol("features")
  .setVocabSize(3)
  .setMinDF(2)
  .fit(df)

// alternatively, define CountVectorizerModel with a-priori vocabulary
val cvm = new CountVectorizerModel(Array("a", "b", "c"))
  .setInputCol("words")
  .setOutputCol("features")

cvModel.transform(df).show(false)
cvm.transform(df).show(false)
```

输出：

```
+---+---------------+-------------------------+
|id |words          |features                 |
+---+---------------+-------------------------+
|0  |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
|1  |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
+---+---------------+-------------------------+

+---+---------------+-------------------------+
|id |words          |features                 |
+---+---------------+-------------------------+
|0  |[a, b, c]      |(3,[0,1,2],[1.0,1.0,1.0])|
|1  |[a, b, b, c, a]|(3,[0,1,2],[2.0,2.0,1.0])|
+---+---------------+-------------------------+
```



## [Normalizer](http://spark.apache.org/docs/latest/ml-features.html#normalizer)

[Normalizer](http://spark.apache.org/docs/latest/ml-features.html#normalizer)是一个Transformer，用于转换Vector行的数据集，将每个Vector标准化为具有单位范数。它采用参数p，该参数指定用于归一化的p范数。 （默认情况下，p = 2。）此规范化可以帮助您标准化输入数据并改善学习算法的行为。

```scala
val dataFrame = spark.createDataFrame(Seq(
  (0, Vectors.dense(1.0, 0.5, -1.0)),
  (1, Vectors.dense(2.0, 1.0, 1.0)),
  (2, Vectors.dense(4.0, 10.0, 2.0))
)).toDF("id", "features")

// Normalize each Vector using $L^1$ norm.
val normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .setP(1.0)

val l1NormData = normalizer.transform(dataFrame)
println("Normalized using L^1 norm")
l1NormData.show()

// Normalize each Vector using $L^\infty$ norm.
val lInfNormData = normalizer.transform(dataFrame, normalizer.p -> Double.PositiveInfinity)
println("Normalized using L^inf norm")
lInfNormData.show()

// Normalize each Vector using $L^2$ norm.
val l2NormData = normalizer.transform(dataFrame, normalizer.p -> 2.0)
println("Normalized using L^2 norm")
l2NormData.show()
```

输出：

```
Normalized using L^1 norm
+---+--------------+------------------+
| id|      features|      normFeatures|
+---+--------------+------------------+
|  0|[1.0,0.5,-1.0]|    [0.4,0.2,-0.4]|
|  1| [2.0,1.0,1.0]|   [0.5,0.25,0.25]|
|  2|[4.0,10.0,2.0]|[0.25,0.625,0.125]|
+---+--------------+------------------+

Normalized using L^inf norm
+---+--------------+--------------+
| id|      features|  normFeatures|
+---+--------------+--------------+
|  0|[1.0,0.5,-1.0]|[1.0,0.5,-1.0]|
|  1| [2.0,1.0,1.0]| [1.0,0.5,0.5]|
|  2|[4.0,10.0,2.0]| [0.4,1.0,0.2]|
+---+--------------+--------------+

Normalized using L^2 norm
+---+--------------+-----------------------------------------------------------+
|id |features      |normFeatures                                               |
+---+--------------+-----------------------------------------------------------+
|0  |[1.0,0.5,-1.0]|[0.6666666666666666,0.3333333333333333,-0.6666666666666666]|
|1  |[2.0,1.0,1.0] |[0.8164965809277261,0.4082482904638631,0.4082482904638631] |
|2  |[4.0,10.0,2.0]|[0.3651483716701107,0.9128709291752769,0.18257418583505536]|
+---+--------------+-----------------------------------------------------------+

```



## IndexToString

与StringIndexer对称，[IndexToString](http://spark.apache.org/docs/latest/ml-features.html#indextostring)将一列标签索引映射回包含原始标签为字符串的列。一个常见的用例是使用StringIndexer从标签生成索引，使用这些索引训练模型，并使用IndexToString从预测索引的列中检索原始标签。但是，您可以自由提供自己的标签。

```scala
val df = spark.createDataFrame(Seq(
  (0, "a"),
  (1, "b"),
  (2, "c"),
  (3, "a"),
  (4, "a"),
  (5, "c")
)).toDF("id", "category")

val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIndex")
  .setStringOrderType("alphabetAsc")
  .fit(df)
val indexed = indexer.transform(df)
indexed.show()

val converter = new IndexToString()
  .setInputCol("categoryIndex")
  .setOutputCol("originalCategory")

val converted = converter.transform(indexed)
converted.select("id", "categoryIndex", "originalCategory").show()
```

输出：

```
+---+--------+-------------+
| id|category|categoryIndex|
+---+--------+-------------+
|  0|       a|          0.0|
|  1|       b|          1.0|
|  2|       c|          2.0|
|  3|       a|          0.0|
|  4|       a|          0.0|
|  5|       c|          2.0|
+---+--------+-------------+

+---+-------------+----------------+
| id|categoryIndex|originalCategory|
+---+-------------+----------------+
|  0|          0.0|               a|
|  1|          1.0|               b|
|  2|          2.0|               c|
|  3|          0.0|               a|
|  4|          0.0|               a|
|  5|          2.0|               c|
+---+-------------+----------------+
```



## VectorAssembler

[VectorAssembler](http://spark.apache.org/docs/latest/ml-features.html#vectorassembler)是一种转换器，它将给定的列的列表组合为单个向量列。这对于将原始特征和由不同特征转换器生成的特征组合到单个特征向量中很有用，以便训练诸如逻辑回归和决策树之类的ML模型。 VectorAssembler接受以下输入列类型：所有数字类型，布尔类型和向量类型（不接受string类型，string类型需要先转换）。在每一行中，输入列的值将按指定顺序连接到向量中。

```scala
val dataset = spark.createDataFrame(
  Seq((0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0))
).toDF("id", "hour", "mobile", "userFeatures", "clicked")
dataset.show(false)

val assembler = new VectorAssembler()
  .setInputCols(Array("hour", "mobile", "userFeatures"))
  .setOutputCol("features")

val output = assembler.transform(dataset)
println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column 'features'")
output.select("features", "clicked").show(false)
// $example off$
```

输出结果：

```
+---+----+------+--------------+-------+
|id |hour|mobile|userFeatures  |clicked|
+---+----+------+--------------+-------+
|0  |18  |1.0   |[0.0,10.0,0.5]|1.0    |
+---+----+------+--------------+-------+

+-----------------------+-------+
|features               |clicked|
+-----------------------+-------+
|[18.0,1.0,0.0,10.0,0.5]|1.0    |
+-----------------------+-------+
```



## VectorIndexer

[VectorIndexer](http://spark.apache.org/docs/latest/ml-features.html#vectorindexer)帮助索引Vector数据集中的分类特征。它既可以自动确定哪些要素是分类的，又可以将原始值转换为分类索引。具体来说，它执行以下操作：

* 采取类型为Vector的输入列和参数maxCategories。
* 根据不同值的数量确定应分类的要素，其中最多具有maxCategories的要素被声明为分类。
* 为每个分类特征计算从0开始的分类索引。
* 为分类特征建立索引，并将原始特征值转换为索引。

具体做法是通过设置一个maxCategories，特征向量中某一个特征不重复取值个数小于maxCategories，则被重新编号为0～K（K<=maxCategories-1）。某一个特征不重复取值个数大于maxCategories，则该特征视为连续值，不会重新编号（不会发生任何改变）。

在transform阶段，VectorIndexer并不像StringIndexer那样具备处理fit时没有碰到过的离散属性值(连续值不影响)或者Null值的能力，如果出现就会抛出 `java.util.NoSuchElementException: key not found: xx`异常。

索引分类特征允许诸如决策树和树组合之类的算法适当地处理分类特征，从而提高性能。

```scala
import org.apache.spark.ml.feature.VectorIndexer

val data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

val indexer = new VectorIndexer()
  .setInputCol("features")
  .setOutputCol("indexed")
  .setMaxCategories(10)

val indexerModel = indexer.fit(data)

val categoricalFeatures: Set[Int] = indexerModel.categoryMaps.keys.toSet
println(s"Chose ${categoricalFeatures.size} " +
  s"categorical features: ${categoricalFeatures.mkString(", ")}")

// Create new column "indexed" with categorical values transformed to indices
val indexedData = indexerModel.transform(data)
indexedData.show()
```

