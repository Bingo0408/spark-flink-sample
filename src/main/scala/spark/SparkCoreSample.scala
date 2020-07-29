package spark

import java.io.{File, PrintWriter}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

object SparkCoreSample {

  /** Main program method */
  def main(args: Array[String]): Unit = {

    // Spark不支持读取网络文件，保存文件到本地
    val fileName = "cars.csv"
    val csv = scala.io.Source.fromURL("https://raw.githubusercontent.com/Bingo0408/data_repo/master/" + fileName).mkString
    val writer = new PrintWriter(new File(fileName))
    writer.write(csv)
    writer.close()

    // 初始化SparkSession以及SparkContext
    // 此外，SparkContext还可以用new SparkContext(conf)的方式初始化
    val conf : SparkConf = new SparkConf()
        conf.setAppName("Simple Spark Core Application")
            .setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext

    // 读取CSV文件，返回RDD
    val rdd = sparkContext.textFile(fileName)
    rdd.foreach(println)

    // 读取CSV文件，返回DataFrame
    val dataframe = sparkSession.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").csv(fileName)

    // 转换DataFrame列类型，可选有integer,double,date,timestamp
    dataframe.select(dataframe.col("mpg").cast("integer"), dataframe.col("weight").cast("double")).printSchema()
    // 显示DataFrame内容
    dataframe.show()
    // 打印Schema信息
    dataframe.printSchema()
    // 按where条件进行过滤，filter方法效果相同
    dataframe.where("engine=307 or weight=3693").show()
    dataframe.filter("engine=307 or weight>4732").show()
    // 选取列
    dataframe.select("mpg","weight","acceleration").show()
    // 将acceleration列值+1
    dataframe.select(dataframe("mpg"),dataframe("weight"),dataframe("acceleration") + 1).show()
    // 将列取别名以及使用函数
    dataframe.selectExpr("mpg", "weight as new_weight", "round(acceleration) as new_acceleration").show()
    dataframe.withColumnRenamed("mpg", "mpg_rename")
    // 追加一列
    dataframe.withColumn("new_mpg", dataframe("mpg"))
    // 删除字段
    dataframe.drop("mpg").show()
    // 排序（从Schema看出，列类型为String因此按照字典序排序）
    dataframe.sort(dataframe("mpg").desc, dataframe("weight").asc).show()
    // 聚合（聚合函数有min,count等等）
    dataframe.groupBy("mpg","weight").count().show()
    dataframe.agg("mpg"->"count", "weight"->"max").show()
    // 去掉重复行，按照某列字段去重
    dataframe.distinct().show()
    dataframe.dropDuplicates("mpg","horsepower").show()
    // Join操作，inner, left等
    // 略（.join)
    // 获取两个DataFrame的交叉记录
    // 略（.intersect）
    // 获取在DF2中不存在的DF1的记录
    // 略（.except）
    // 按照空格分割name列并生成新的"行"
    dataframe.explode("name", "name_") {name: String => name.split(" ")}.show()

    // 注册为临时表，表名会自动转为小写
    dataframe.createOrReplaceTempView("Sample_DataFrame")
    // 列出所有SparkSession Catalog中注册的表
    sparkSession.catalog.listTables().show()
    sparkSession.sql("show tables").show()
    // 执行sql语句
    sparkSession.sql("select * from sample_dataframe").show()

    // 注册UDF函数
    def appendPrefix(s: String): String = "prefix_" + s
    sparkSession.udf.register("prefix", appendPrefix _)
    sparkSession.sql("select prefix(mpg) from sample_dataframe").show()

    // 查看某些列之间的关联度，协方差等
    println(dataframe.stat.corr("weight", "acceleration"))
    println(dataframe.stat.cov("weight", "acceleration"))

    /** 1.将DataFrame的行转换为Car Bean
      * 2.调用convertCar2Benz()方法将Car转化为Benz */
    def convertCar2Benz(car: Car) = { val benz = new Benz(car.weight, "benz"); benz}
    import sparkSession.implicits._
    val dataset: Dataset[Car] = dataframe.as[Car]
    dataset.printSchema()
    dataset.show()
    dataset.map(car => (car.mpg, convertCar2Benz(car))).show()
  }
  // 主类声明体外定义CSV的每行代表的Bean对象，这里是Car。转换后的对象为Benz
  // DataSet的定义Bean中的变量名必须要和DataFrame的Column名一致，否则无法自动填装。自定义Car Bean中未必需要定义所有的字段
  case class Car(mpg: String, cylinders: Int, engine: Double, horsepower: String, weight: Int, acceleration: Double, year: Int, origin: String, name: String)
  case class Benz(weight: Int, name: String)
}
