package flink

import java.io.{File, PrintWriter}

import org.apache.flink.api.scala._

object FlinkBatchSample {

  /** Main program method */
  /** Flink程序开发的一般步骤
    * 1.获取执行环境execution environment
    * 2.load/init数据
    * 3.声明transformation处理
    * 4.声明数据结果存放
    * 5.启动程序执行
    * 注意点：Flink数据模型不是建立在Key-Value对上的，因此不需要物理上pack数据。Key是虚拟。
    */
  def main(args: Array[String]) : Unit = {

    /** Flink不支持读取网络文件，保存文件到本地 */
    val fileName = "us_states.csv"
    val file = scala.io.Source.fromURL("https://raw.githubusercontent.com/Bingo0408/data_repo/master/" + fileName).mkString
    val writer = new PrintWriter(new File(fileName))
    writer.write(file)
    writer.close()

    /** 初始化Flink执行环境
      * 一般使用getExecutionEnvironment即可，它会自动根据你的环境生成定义 */
    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment

    /** 读取文件的两种方式 readTextFile/readCsvFile */
    /** 1.以纯文本方式读取本地文件 返回为DataSet==RDD */
    val text = env.readTextFile(fileName)
    /** 2.以CSV格式按照列读取CSV, ignoreFirstLine=true忽略Header, includedFields = Array(1, 4, 6)只读取第2,5,7列 */
    //  其他方式env.readCsvFile[(Int, String, Double)](fileName)
    case class UStates(state: String, population: Int, area: Int)
    val csv = env.readCsvFile[UStates](fileName, includedFields = Array(1, 4, 6), ignoreFirstLine=true)

    /** WordCount样例1，按Tuple的field0执行group，field1执行sum */
    text.flatMap(_.toLowerCase.split("\\W+")).filter(_.nonEmpty).map((_,1)).groupBy(0).sum(1).print()
    /** WordCount样例2，组装为WordCountPair并自定义reduce函数, .groupBy("word")直接指定field也正确 */
    case class WordCountPair(word: String, cnt: Int)
    text.flatMap(_.toLowerCase.split("\\W+")).map(WordCountPair(_, 1)).groupBy(_.word).reduce((w1, w2) => WordCountPair(w1.word, w1.cnt + w2.cnt)).print()
    /** 其他的DataSet API
      * 1.reduceGroup是reduce的升级方案，先做组内reduce然后是整体reduce
      * 2.cogroup是join的一般实现 */

    /** 工号、姓名、年龄 */
    val dataset1 : DataSet[(String, String, Int)] = env.fromElements(
      ("0001", "name1", 31),
      ("0002", "name2", 32),
      ("0003", "name3", 33),
      ("0005", "name5", 35))

    /** 工号、姓名、年龄 */
    val dataset2 : DataSet[(String, String, Int)] = env.fromElements(
      ("0001", "name1", 10001),
      ("0002", "name2", 10002),
      ("0003", "name3", 10003),
      ("0004", "name4", 10004))

    dataset1.coGroup(dataset2).where(0).equalTo(0).collect().foreach(x => print(x.productArity))
  }
}