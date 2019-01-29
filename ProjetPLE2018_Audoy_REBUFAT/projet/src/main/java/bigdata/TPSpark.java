package bigdata;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;



public class TPSpark {

	private static final String PathDir = "dem3/";
	private static final int Dem3Size = 1201; 
	public static final double RADIUS = 6378137.0; /* in meters on the equator */
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

		Convertor convertor = new Convertor();
		Aggregation aggreg = new Aggregation();
		colDef.generatePalette(min, max);

		aggreg.setMaxZoom(MAXZOOMLEVEL);

		//JavaPairRDD<String, PortableDataStream> pairRddBinLinked = Aggregation.setAggregation(pairRddBin);

		JavaPairRDD<String, short[]> pairRddConvert = convertor.convertHgtToHigh(pairRddBin, Dem3Size, Dem3Size);

		//JavaPairRDD<String, Iterable<short[]>> pairRddGroupedTiles = pairRddConvert.groupByKey();

		JavaPairRDD<String, byte[]> pairRDDTilesPng = convertor.convertToPNG(pairRddConvert, Dem3Size, Dem3Size);

		Connection connection = null;
		try {
			connection = connect.connectTable();
			connect.createTable(connection);
			pairRDDTilesPng = pairRDDTilesPng.mapToPair(file -> {
				return new Tuple2<String, byte[]>(strUtils.extractNameFromPath(file._1) + "Z1", file._2);
			});

			ConnectionToHBase connec = new ConnectionToHBase();

			connec.SavesTiles(pairRDDTilesPng);

			connection.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}



		context.close();

	}

}
