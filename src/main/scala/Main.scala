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
            
        sparkCovidData()


        spark.stop()


        def sparkCovidData():Unit = {
            println("====================")
            println("Welcome to Team 1's Spark Covid Data Analysis")
            println("====================")
            var endProgram = false
            while (!endProgram){
                mainmenu()
            }
        }

        def mainmenu():Unit = {
            println
        }



    }
}
