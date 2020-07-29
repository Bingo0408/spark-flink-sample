package spark

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object SparkStreamSample {

  /** Main program method */
  def main(args: Array[String]): Unit = {

    // 初始化StreamContext，以5秒为间隔执行流计算
    val conf : SparkConf = new SparkConf()
    conf.setAppName("Simple Spark Stream Application")
        .setMaster("local[*]")  // 只有本地运行时设置该参数，集群模式一般由shell参数指定
    val streamContext = new StreamingContext(conf, Seconds(5))
    // 设置日志等级，使输出更加清晰
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)

    /** 1.无状态转换操作样例代码
    // 监听套接字窗口信息
    val socketContent = streamContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)
    // 打印WordCount信息
    socketContent.flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _).print()
    // 启动Stream
    streamContext.start()
    streamContext.awaitTermination() */

    // 监听套接字窗口信息
    val socketContent = streamContext.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    /** 2.有状态转换操作样例代码 */
    /** 2.1 基于滑动窗口的函数说明
      * 1.reduceByKeyAndWindow有一个更高效的实现：它基于每次进入的窗口的数据进行增量计算，离开窗口的数据进行相减操作。对于窗口跨度大的流计算很高效
      * 例如.socketContent.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y,Seconds(30), Seconds(5)).print()
      * 2.window
      * 3.countByWindow
      * 4.reduceByWindow
      * 5.countByValueAndWindow
    // 设置检查点（容错机制），支持HDFS
    streamContext.checkpoint("./checkpoint/")
    // 定义滑动窗口的宽度为30秒，每次移动5秒。如果两者相等的话，类似于只计算自己的批次
    socketContent.flatMap(_.split(" ")).map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => x + y, Seconds(30), Seconds(5)).print()
    // 启动Stream
    streamContext.start()
    streamContext.awaitTermination() */

    /** 2.2 基于跨批次的状态操作
      * updateStateByKey[T](updateFunc)把DStream中的数据按Key做Reduce操作，然后对各个批次的数据进行累加
      * 其中updateFunc的第一个参数是该Key新增的值的集合（Seq[T]形式），
      *                第二个参数是这个Key对应的之前所有批次的值的总和，即当前保存的状态（Option[T]形式）*/
    // 设置检查点（容错机制），支持HDFS
    streamContext.checkpoint("./checkpoint/")

    //定义更新函数
    val updateFunc = (values: Seq[Int], state: Option[Int]) => {
      // 之前批次该Key出现的次数，若未出现置为0
      val previousCnt = state.getOrElse(0)
      // 当前批次该Key新增的值的累加（fold函数定义了累加并将初始值定位0）
      val currentCnt = values.foldLeft(0)(_ + _)
      // 返回结果
      Some(currentCnt + previousCnt)
    }
    // 统计历史上所有单词的词频
    socketContent.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc).print()

    // 启动Stream
    streamContext.start()
    streamContext.awaitTermination()
  }
}
