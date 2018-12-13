package bigdata;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class TPSpark {

	public static void main(String[] args) {
		
		
		
		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		
		JavaPairRDD<String, String> filePairRddSS = context.wholeTextFiles("hdfs://young:9000/user/raw_data/dem3/");
		
		JavaRDD<String> cle = filePairRddSS.values();
		
		System.out.println(cle.count());
		/*
		JavaPairRDD<String, Tuple2<Integer, Integer>> filePairRddSTII = filePairRddSS.keys().mapToPair(new PairFunction<String, String, Tuple2<Integer, Integer>>(){
			public Tuple2<String, Tuple2<Integer,Integer>> call(String line){
				String[] tokens=line.split("/");
				String name=tokens[tokens.length-1].split(".")[0];
				int lat=Integer.parseInt(name.substring(1, 3));
				int lon=Integer.parseInt(name.substring(4,7));
				if(name.charAt(0)=='S' || name.charAt(0)=='s') lat*=-1;
				if(name.charAt(0)=='W' || name.charAt(0)=='w') lon*=-1;
				return new Tuple2<String, Tuple2<Integer, Integer>>(line,  new Tuple2<Integer, Integer>(lat, lon));
			}
		});
		
		System.out.println(filePairRddSTII.first()._1);
		System.out.println(filePairRddSTII.first()._2._1+" "+filePairRddSTII.first()._2._2);*/
	}
	
}
