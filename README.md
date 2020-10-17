# project202010
load data to mysql
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object LoadToMysql extends App {

  val spark = SparkSession.builder
    .master("local[2]")
    .appName("appName")
    .getOrCreate()
  val sc=spark.sparkContext
  private val sourceDF: DataFrame = spark.read.format("csv").option("header", "true").load(".idea\\data\\student_score.csv")
  sourceDF.show(20)
  val tableName2 = sourceDF.createOrReplaceTempView("student_score")
  val url = "jdbc:mysql://localhost:3306/l_lunches"
  val tableName = "student_score"
  // 设置连接用户、密码、数据库驱动类
  val prop = new java.util.Properties
  prop.setProperty("user","root")
  prop.setProperty("password","123456")
  prop.setProperty("driver","com.mysql.jdbc.Driver")
  sourceDF.write.mode("append").jdbc(url,"stud_score",prop)
  /*private val df1: DataFrame = spark.sql("select stu_id ,stu_name,\ncase when cou_name = \"hadoop\"  then score end cou_hadoop,\nfrom student_score")
  df1.createOrReplaceTempView("student_1")
  spark.sql("select * from student_1").show()*/

}
