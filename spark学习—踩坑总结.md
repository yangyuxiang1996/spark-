# 数一数踩过的坑

## 空指针

报错：Caused by: java.lang.NullPointerException: Value at index 1 is null

产生原因：

一般是使用getAs[Int]或者getInt时，对应的位置为null，所以需要使用isNullAt()方法进行判断

但是使用getString方法时不会产生NullPointerException，null对应的位置仍然是null（注意不是str类型）