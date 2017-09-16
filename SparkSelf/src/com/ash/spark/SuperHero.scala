package com.ash.spark

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level


object SuperHero {

	def parseName(lines : String): Option[(Int, String)]={

			val fields = lines.split('\"')
					if (fields.length > 1 ){
						return Some(fields(0).trim().toInt, fields(1)) 
					}else{
						return None
					}

	}
	
	def countOccurance(line:String)={
	  
	  val elements = line.split("\\s+")
	  (elements(0).trim().toInt, elements.length-1)
	}


	def main(args: Array[String]): Unit = {

			Logger.getLogger("org").setLevel(Level.ERROR)

			val spark = SparkSession.builder().
			master("local[*]").
			appName("SuperHero").
			config("spark.some.config.option", "some-value").
			getOrCreate()

			val sc = spark.sparkContext

			val names = sc.textFile("../marvel-names.txt")
      
			//caling flatMap on names since file may have empty data, which will be eliminated using flatMap > Option 
			val nameRDD =  names.flatMap(parseName)
			
			val lines = sc.textFile("../marvel-graph.txt")
			
			val occurance = lines.map(countOccurance)
			
			val totalByCharacter = occurance.reduceByKey((x,y) => x + y)
			
			val flipped = totalByCharacter.map(x => (x._2, x._1))
			
			val mostPopular = flipped.max()
			
			// lookup will return an array so we have to use (0) to extract only value
			val mostPopularName = nameRDD.lookup(mostPopular._2)(0)
			
			println(s"mostpoplular superhero is $mostPopularName with ${mostPopular._1} co-occurances")
			
			
			



	}

}