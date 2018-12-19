package bigdata;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;



public class TPSpark {

	private static final String PathDir = "dem3/";
	

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);

//		FileSystem fs = null;
//		FileStatus [] arrayFiles = null ;
//		ArrayList<String> pathFile = new ArrayList<>();
//
//		try {
//			fs = FileSystem.get(new Configuration());
//			arrayFiles = fs.listStatus(new Path(PathDir));
//			for(FileStatus status : arrayFiles) {
//				pathFile.add(status.getPath().toString());
//			}
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();			
//		} catch (IllegalArgumentException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} 
 
//		JavaRDD<String> rddFileName = context.parallelize(pathFile).cache();
		
		JavaPairRDD<String, PortableDataStream> rddBin=context.binaryFiles(PathDir);
		rddBin=rddBin.filter(file -> {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus fileStatus=fs.getFileStatus(new Path(file._1));
			return fileStatus.getModificationTime() < Time.now();
		});
		
		Connection c;
		try {
			c = ConnectionToHBase.connectTable();
			ConnectionToHBase.createTable(c);
			c.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		rddBin.foreach(f->{
			Connection c2=ConnectionToHBase.connectTable();
			byte[] data = f._2.toArray();
			ConnectionToHBase.putData(c2, f._1, data);
			c2.close();
			
		});
		//TODO rdd.filter a faire
//		rddBin=rddBin.map(file ->{
//			String tab[] =StringUtils.extractLonLat(file._1);
//			
//		});
		
//		rddFileName.foreach(path -> {
//			System.out.println("<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>"+path);
//			//File f=new File(new URI(path));
//			//System.out.println("<<<<<<<<<<<<<<<<<<<<<<>>>>>>>>>>>>>>>>>>>>"+f.getAbsolutePath());
//			//InputStream input = new  FileInputStream(f);
//			
//			//byte [] high = null;
//			
//			
//			//input.close();
//		});
		

//		System.out.println(rddFileName.count());
		context.close();

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
