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
            println("Welcome to Team 1's Spark Covid Data Analysis ")
            println("====================")
            var endProgram = false
            while (!endProgram){
                mainmenu()
            }
        }

        def mainmenu():Unit = {
            println
        }

        //Method to calculate the median age of vaccinated people related to their location
        //Fields: Median_Age, Total_Vaccinations, Location
        def MedianAgeOfVaccinatedPeopleBasedOffLocation():UNIT 
        {


        }

        //Method to calculate the number of deaths among vaccinated people between the age of 65 and 70
        //Fields: New_Deaths, People_Vaccinated, Total_Vaccinations
        def DeathsAmongVaccinatedPeopleBetweenAges65And70():UNIT 
        {


        }


        //Method to calculate the median age of death
        //Fields: Median_Age, Total_Deaths
        def MedianAgeOfDeath():UNIT 
        {


        }

        //Method to calculate the number of new cases in people 70 and older and also their mortality rate
        //Fields: New_Cases, Aged_70_Older, New_Deaths, Total_Deaths
        def NewCasesInPeople70Plus():UNIT 
        {


        }

        //Method to calculate percentage of deaths and vaccinations in a contintent and comapare
        //Fields: Continent, Total_Deaths, People_Vaccinated
        def DeathsVSVaccinationPerContinent():UNIT 
        {


        }

        //Method to calculate the life expectancy of people 70 and up who contracted the virus
        //Fields: Life_Expectancy, Aged_70_Older
        def LifeExpectancyOfPeople70Plus():UNIT 
        {


        }


        //Method to calculate which continent had the largest number of cases in a 30 day period
        //Fields: Location, Total_Cases, New_Cases, Date
        def ContinentWithMostCasesInA30DayPeriod():UNIT 
        {


        }

        //Method to calculate all this stuff
        //Fields: New_Cases, Total_Cases, New Deaths, Total_Deaths, Aged_65_Older, Continent, Location
        def NewAndTotalCases_NewAndTotalDeaths_InPeople65Plus_PerContinent():UNIT 
        {


        }

        //Method to calculate Population Density and compare it to Total Vaccinations, Cases, and Deaths
        //Fields: Population_Density, Population, Continent, Location, Date, Total_Cases, New_Cases, Total_Deaths
            //New_Deaths, New_Tests, Total_Tests, Total_Vaccinations
        def PopulationDensityVsTotalVaccination_Cases_Deaths():UNIT 
        {


        }

        //Method to calculate total Covid cases in places where more then half of the population is vaccinated
        //Fields: Population, Location, New_Cases, People_Fully_Vaccinated
        def TotalCovidCasesInLocationsWhereTotalVaccinationRateIsAbove50Percent():UNIT 
        {


        }






    }
}
