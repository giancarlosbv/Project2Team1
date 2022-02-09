import org.apache.spark.sql.SparkSession

object Main {
    def main(args:Array[String]): Unit = {
        val spark = 
            SparkSession
            .builder
            .appName("SparkHelloWorld")
            .config("spark.master", "local")
            .config("spark.eventLog.enabled", "false")
            .getOrCreate()
            
        println("===================")
        println("Hello Spark!")
        println("===================")
        spark.stop()
    }
}
