import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class HeroesStreaming {
	
	public static void main(String[] args) {

	
	Logger.getLogger("org.apache").setLevel(Level.OFF);
	Logger.getLogger("appex").setLevel(Level.OFF);

	SparkSession spark = SparkSession.builder().appName("SparkStreaming").master("local[*]")
			.config("spark.sql.warehouse.dir","file///c:/tmp/").getOrCreate();
	
	
	JavaStreamingContext jssc = new JavaStreamingContext(new JavaSparkContext(spark.sparkContext()), Durations.milliseconds(1000));
	
	
	String filepath = "src/main/resources/heroes_information.csv";
	

										 
	JavaDStream<String> stream = jssc.receiverStream(new CustomReceiver(filepath))
										 	 .filter(str -> !(null==str))
											 .filter(str ->!str.contains("id"));
													 
										 
										 

									
	

	

	JavaDStream<Object> heroDStreams1 = stream .map(str -> Heroes.parseRating(str))
            .filter(hero->hero.getGender().equals("Female") && hero.getRace().equals("Human"))
            .map(f-> "Hero Name :"+f.getName() + "  | Hero gender : " + f.getGender());

	heroDStreams1.countByWindow(Durations.seconds(3000), Durations.seconds(3000)).countByValue();


	heroDStreams1.print();

	
	

	JavaDStream<Object> heroDStreams2 = stream.map(str -> Heroes.parseRating(str))
            .filter(hero->hero.getWeight()>100 && hero.getAlignment().equals("good"))
            .map(f-> "Hero Name : "+f.getName() +
    				" , " + "Alignment :"+ f.getAlignment() +
    				" , " + "Weight :"+ f.getWeight());
		heroDStreams2.countByValueAndWindow(Durations.seconds(3000), Durations.seconds(3000));
		heroDStreams2.print();
	
	
	
	JavaDStream<Object> heroDStreams3 = stream .map(str -> Heroes.parseRating(str))
            .filter(hero-> hero.getHaircolor().equals("No Hair"))
            .map(s-> "Hero name : "+s.getName()+ " | Hair : "+ s.getHaircolor());

	heroDStreams3.countByWindow(Durations.seconds(3000), Durations.seconds(3000)).countByValue();
	heroDStreams3.print();

	
	
	JavaDStream<Object> heroDStreams4 = stream .map(str -> Heroes.parseRating(str))
            .filter(hero-> hero.getPublisher().equals("DC Comics"))
            .map(s-> "Movie name : "+ s.getName());

	heroDStreams4.countByValueAndWindow(Durations.seconds(3000), Durations.seconds(3000));

	heroDStreams4.print();
	
	
	
	JavaDStream<Object> heroDStreams5 = stream .map(str -> Heroes.parseRating(str))
            .filter(hero-> hero.getEyecolor().equals("blue") && hero.getHaircolor().equals("Blond"))
            .map(s-> "Movie name : "+ s.getName());
	heroDStreams5.countByWindow(Durations.seconds(3000), Durations.seconds(3000)).countByValue();

	
	heroDStreams5.print();


	
	try {
		
		jssc.start();
		
		jssc.awaitTermination();
		
	} catch (InterruptedException e) {
		System.out.println("called"+e.getMessage());
		e.printStackTrace();
	}
	}

	//jssc.close();
}

