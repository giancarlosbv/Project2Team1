
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SQLContext
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.SparkSession
import java.util.Scanner
import scala.util.matching.Regex.Match
object Main {
    def main(args:Array[String]): Unit = {

        System.setSecurityManager(null)
        System.setProperty("hadoop.home.dir", "C:\\hadoop\\") 
        val conf = new SparkConf()
            .setMaster("local") 
            .setAppName("Project2Team1")    
        val sc = new SparkContext(conf)
        sc.setLogLevel("ERROR")
        val hiveCtx = new HiveContext(sc)
        import hiveCtx.implicits._
        val spark = 
            SparkSession
            .builder
            .appName("Project2Team1")
            .config("spark.master", "local")
            .config("spark.eventLog.enabled", "false")
            .getOrCreate()
            
        
        

        sparkCovidData() //calling method that holds all of what's happening in our code


        spark.stop()

    def insertCovidData(hiveCtx:HiveContext): Unit = 
    {
        hiveCtx.sql("DROP TABLE IF EXISTS Table")
        
        val output = hiveCtx.read
            .format("csv")
            .option("inferSchema", "true")
            .option("header", "true")
            .load("input/covid-data.csv")
        

    
        output.createOrReplaceTempView("temp_data")
        hiveCtx.sql("CREATE TABLE IF NOT EXISTS Table (iso_code STRING, continent STRING, location STRING, date STRING, total_cases INT, new_cases INT, total_deaths INT, new_deaths INT, new_tests INT, total_tests INT, total_vaccinations INT, people_vaccinated INT, people_fully_vaccinated INT, population INT, population_density INT, median_age INT, aged_65_older INT, aged_70_older INT, gdp_per_capita INT, hospital_beds_per_thousand INT, life_expectancy INT )")
        hiveCtx.sql("INSERT INTO Table SELECT * FROM temp_data")
        
        val summary = hiveCtx.sql("SELECT * FROM Table LIMIT 10")


        //summary.show()
    }



        def sparkCovidData():Unit = {
            
            insertCovidData(hiveCtx)

            var scanner = new Scanner(System.in)
            println("====================")
            println("Welcome to Team 1's Spark Covid Data Analysis")
            var endProgram = false //endProgram boolean flag, set to false, if ever set to true, program will end as defined by while loop and use of ! operator. 
            while (!endProgram){ //while endProgram is NOT true, loop executes. if endProgram IS true, loop will not execute
                mainMenu()
            }

            def mainMenu():Unit = {
            println("====================") //start every method or new prints with 10 "=" for readability, separation of things
            println("Main menu")
            println("1. Median Age Of Vaccinated People Based Off Location")
            println("2. Deaths Among Vaccinated People Between Ages 65 And 70")
            println("3. Median Age Of Death")
            println("4. New Cases In People 70 Plus")
            println("5. Deaths Vs. Vaccinations Per Continent")
            println("6. LifeExpectancyOfPeople70Plus")
            println("7. Continent With Most Fully Vaccinated")
            println("8. New and Total Cases, New and Total Deaths by Continent")
            println("9. Population Density Vs Total Vaccinations,Cases, and Deaths")
            println("10. Total Covid Cases In Locations Where Total Vaccination Rate Is Above 50 Percent")
            println("11. Exit")
            var menuChoice = scanner.nextInt()
            scanner.nextLine()
            //match case, similar to if else chain, but a little cleaner and built in error handling through "_" or default case
            menuChoice match {
                case 1 =>
                    MedianAgeOfVaccinatedPeopleBasedOffLocation()
                case 2 =>
                    DeathsAmongVaccinatedPeopleBetweenAges65And70()
                case 3 =>
                    MedianAgeOfDeath()
                case 4 =>
                    top10DeathRatesLocation()
                case 5 =>
                    DeathsVSVaccinationPerContinent()
                case 6 =>
                    LifeExpectancyOfPeople70Plus()
                case 7 =>
                    ContinentWithMostFullyVaccinated()
                case 8 =>
                    NewAndTotalCases_NewAndTotalDeaths_ByContinent()
                case 9 =>
                    PopulationDensityVsNewCaseRate()
                case 10 =>
                    TotalCovidCasesInLocationsWhereTotalVaccinationRateIsAbove50Percent()
                case 11 => //exit program by choosing 11, endProgram boolean is set to true and while look ends.
                    endProgram = true
                case _ => //handles invalid inputs, only valid inputs are the "cases" defined above, any other input will be handled by this
                    println("====================")
                    println("Invalid Input, Try again")
                    mainMenu()
            }

        }
        //Method to calculate the median age of vaccinated people related to their location
        //Fields: Median_Age, Total_Vaccinations, Location
        def MedianAgeOfVaccinatedPeopleBasedOffLocation(): Unit =
        {   
            println("====================")

        val result = hiveCtx.sql("select avg(median_age), sum(total_vaccinations), location, max(people_fully_vaccinated) from Table where median_age is not null group by location order by 1")
        result.show   }

        //Method to calculate the number of deaths among vaccinated people between the age of 65 and 70
        //Fields: New_Deaths, People_Vaccinated, Total_Vaccinations
        def DeathsAmongVaccinatedPeopleBetweenAges65And70():Unit =  
        {
        //this one
        println("====================")

        val result = hiveCtx.sql("select location, sum(new_deaths), sum(people_vaccinated), sum(total_vaccinations), avg(aged_65_older) from Table group by location")
        result.show   
    }


        //Method to calculate the median age of death
        //Fields: Median_Age, Total_Deaths
        def MedianAgeOfDeath():Unit =  
        {
            println("====================")
            

        }

        //Method to calculate the number of new cases in people 70 and older and also their mortality rate
        //Fields: New_Cases, Aged_70_Older, New_Deaths, Total_Deaths
        def top10DeathRatesLocation():Unit =  
        {
            println("====================")
            val result = hiveCtx.sql("Select location, SUM(population), SUM(total_deaths)/SUM(population) AS death_rate from table WHERE date = '2/7/2022' GROUP BY location ORDER by 3 DESC")
            result.show()


        }

        //Method to calculate percentage of deaths and vaccinations in a contintent and comapare
        //Fields: Continent, Total_Deaths, People_Vaccinated
        def DeathsVSVaccinationPerContinent():Unit =  
        {
            println("====================")


        }

        //Method to calculate the life expectancy of people 70 and up who contracted the virus
        //Fields: Life_Expectancy, Aged_70_Older
        def LifeExpectancyOfPeople70Plus():Unit =  
        {
            println("====================")



        }


        //Method to calculate which continent had the largest number of cases in a 30 day period
        //Fields: Location, Total_Cases, New_Cases, Date
        def ContinentWithMostFullyVaccinated():Unit =  
        {
            println("====================")
            val result = hiveCtx.sql("Select location, MAX(people_fully_vaccinated) from Table group by location")
            result.show 
        }

        //Method to calculate all this stuff
        //Fields: new_cases, total_cases, new_deaths, total_deaths, aged_65_older, aged_70_older, continent, location
        def NewAndTotalCases_NewAndTotalDeaths_ByContinent():Unit =  
        {
            println("====================")
            println("New/Total Cases, New/TotalDeaths by continent ")
            val result = hiveCtx.sql("SELECT continent, sum(new_cases), SUM(total_cases), SUM(new_deaths), SUM(total_deaths), Round(SUM(new_cases)/SUM(population), 5) AS newCaseRate, SUM(new_deaths)/SUM(population) AS death_rate FROM table WHERE date = '2/7/2022' AND continent IS NOT NULL GROUP BY continent ORDER BY 2 DESC ")
            result.show()
            result.write.csv("results/NewAndTotalCases_NewAndTotalDeaths_ByContinent")
        }

        //Method to calculate Population Density and compare it to Total Vaccinations, Cases, and Deaths
        //Fields: Population_Density, Population, Continent, Location, Date, Total_Cases, New_Cases, Total_Deaths
            //New_Deaths, New_Tests, Total_Tests, Total_Vaccinations
        def PopulationDensityVsNewCaseRate():Unit =  
        {
            println("====================")
            println("Population Density vs New Case Rate")
            val result = hiveCtx.sql("SELECT location, sum(population), AVG(population_density), SUM(new_cases)/SUM(population) AS NewCaseRate , sum(new_cases) FROM table WHERE date = '2/7/2022' group by location order by 3 DESC ")
            result.show()
            result.write.csv("results/PopulationDensityVsNewCaseRate")

        }

        //Method to calculate total Covid cases in places where more then half of the population is vaccinated
        //Fields: Population, Location, New_Cases, People_Fully_Vaccinated
        def TotalCovidCasesInLocationsWhereTotalVaccinationRateIsAbove50Percent():Unit =  
        {
            println("====================")
            val result = hiveCtx.sql("select sum(population), location, sum(new_cases), AVG(people_fully_vaccinated) from Table group by location") 
            result.show()
        }



    
        }

        








    }
}


    

        





