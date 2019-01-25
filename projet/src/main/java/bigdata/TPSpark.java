package bigdata;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;



public class TPSpark {

	private static final String PathDir = "../raw_data/dem3/";
	private static final int Dem3Size = 1201; 
	private static final int DEM3SIZEREDUCED = 301; //1201 / 4 arrondi au supérieur
	public static final double RADIUS = 6378137.0; /* in meters on the equator */
	private static final int MaxHigh = 8848;
	private static final int Blue = 0x0000FF;
	private static final int Green = 0x00FF00;
	private static final int Red = 0xFF0000;
	private static final int MAXZOOMLEVEL = 8;

	static int min = -420; //environ le point le plus bas de la terre 
	static int max = 8850; //environ la hauteur de l'everst

	private static StringUtils strUtils = new StringUtils();
	private static ColorsDefinition colDef = ColorsDefinition.getInstance();
	private static ConnectionToHBase connect = new ConnectionToHBase();

	public static String convertCoordToCartesian(String coord) {
		String [] coordExtracted = strUtils.extractLonLat(coord);
		Integer lat = Integer.parseInt(coordExtracted[0]);
		Integer lon = Integer.parseInt(coordExtracted[1]);
		int zoom = 1;
		
		if(coord.contains("s") || coord.contains("S")) lat *= -1;
		if(coord.contains("w") || coord.contains("W")) lon *= -1;
		
		Integer x = (int)Math.floor(((lon + 180) / 360) * Math.pow(2,zoom));
		Integer y = (int)Math.floor(
				(1 - Math.log(Math.tan(lat * Math.PI / 180) +
						(1 / Math.cos(lat * Math.PI / 180))) / Math.PI) / Math.pow(2, zoom-1));

		String finalCoord = "X" + x.toString() + "Y" + y.toString() + "Z" + zoom;
		return finalCoord;
	}

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		//context.setLogLevel("warn");

		JavaPairRDD<String, PortableDataStream> pairRddBin=context.binaryFiles(PathDir);

		pairRddBin = pairRddBin.filter(file -> {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus fileStatus=fs.getFileStatus(new Path(file._1));
			return fileStatus.getModificationTime() < Time.now();
		});


		Convertor convertor = new Convertor();
		Aggregation aggreg = new Aggregation();
		colDef.generatePalette(min, max);

		aggreg.setMaxZoom(MAXZOOMLEVEL);

		//JavaPairRDD<String, PortableDataStream> pairRddBinLinked = Aggregation.setAggregation(pairRddBin);

		//Broadcast<ColorsDefinition> colorDefBroadcast = context.broadcast(colDef);

		JavaPairRDD<String, short[]> pairRddConvert = convertor.convertHgtToHigh(pairRddBin, Dem3Size, Dem3Size);



		JavaPairRDD<String, Iterable<short[]>> pairRddGroupedTiles = pairRddConvert.groupByKey();


		JavaPairRDD<String, byte[]> pairRDDTilesPng = convertor.convertToPNG(pairRddConvert, Dem3Size, Dem3Size);



		/**
		 * Ici on aggrege les tuiles. 
		 */
		/*pairRDDTilesPng = pairRDDTilesPng.mapToPair( tile ->{
			String tilePos = tile._1;
			String[] tilePosParsed = tilePos.split("N:n:S:s:E:e:W:w:z:Z");
			int lat = Integer.parseInt(tilePosParsed[0]);
			int lng = Integer.parseInt(tilePosParsed[1]);
			int xToAgregate = lat;
			int yToAgregate = lng;
			if(lat % 2 != 0)
				xToAgregate -=1;
			if( lng % 2 != 0)
				yToAgregate -= 1;

			String tileToAggregate = "x" + xToAgregate + "y" + yToAgregate;
			return new Tuple2<String, byte[]>(tileToAggregate, tile._2);
		});*/

		/**
		 * Save To Hbase
		 */
		Connection connection = null;
		try {
			connection = connect.connectTable();
			connect.createTable(connection);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		/*pairRDDTilesPng.foreach(png -> 	{
			System.out.println(convertCoordToCartesian(strUtils.extractNameFromPath(png._1)));
			ByteArrayInputStream bis = new ByteArrayInputStream(png._2);
			BufferedImage img = ImageIO.read(bis);
			ImageIO.write(img, "png", new File(png._1 + ".png"));
		});*/

		pairRDDTilesPng = pairRDDTilesPng.mapToPair(file -> {
			return new Tuple2<String, byte[]>(strUtils.extractNameFromPath(file._1) + "Z1", file._2);
		});

		ConnectionToHBase connec = new ConnectionToHBase();




		try {
			connec.SavesTiles(pairRDDTilesPng);

			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		context.close();

		//TODO rdd.filter a faire


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
		});*/
	}

}
