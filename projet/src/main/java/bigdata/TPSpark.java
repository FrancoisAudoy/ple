package bigdata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;



public class TPSpark {
	static String PathDir = "dem3/";
	
	public static void main(String[] args) {



		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);

		FileSystem fs = null;
		FileStatus [] listFile = null ;
		ArrayList<String> pathFile = new ArrayList<>();
		
		try {
			fs = FileSystem.get(new Configuration());
			listFile = fs.listStatus(new Path(PathDir));
			for(FileStatus status : listFile)
				pathFile.add(status.getPath().toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();			
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		JavaRDD<String> rddFileName = context.parallelize(pathFile);

		System.out.println(rddFileName.count());

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
