import org.apache.spark.sql.SparkSession
import java.util.Scanner
import scala.util.matching.Regex.Match
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
            var scanner = new Scanner(System.in)

            println("====================")
            println("Welcome to Team 1's Spark Covid Data Analysis")
            var endProgram = false
            while (!endProgram){
                mainMenu()
            }

            def mainMenu():Unit = {
            println("====================")
            println("Main menu")
            var menuChoice = scanner.nextInt()
            scanner.nextLine()
            menuChoice match {
                case 1 =>
                    println("case 1")
                case 2 =>
                    println("case 2")
                case 3 =>
                    println("case 3")
                case 4 =>
                    println("case 4")
                case 5 =>
                    println("case 5")
                case 6 =>
                    println("case 6")
                case 7 =>
                    println("case 7")
                case 8 =>
                    println("case 8")
                case 9 =>
                    println("case 9")
                case 10 =>
                    println("case 10")
                case 11 =>
                    endProgram = true
                case _ =>
                    println("Invalid Input, Try again")
                    mainMenu()
            }

        }
        }

        



    }
}
