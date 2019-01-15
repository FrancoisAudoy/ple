package bigdata;

import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Time;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;



public class TPSpark {

	private static final String PathDir = "dem3/";
	private static final int Dem3Size = 1201; //On va tricher un peu
	private static final int MaxHigh = 8848;
	private static final int Blue = 0x0000FF;
	private static final int Green = 0x00FF00;
	private static final int Red = 0xFF0000;

	static int min = Integer.MAX_VALUE;
	static int max = Integer.MIN_VALUE;


	public static int[] ConvertHighToRGB(short [] high) {
		int [] result = new int [high.length];

		for(int i = 0; i < high.length; ++i) {
			short loged = high[i]; //(int) Math.log(high[i]) / (int) Math.log(MaxHigh);
			result[i] = StringUtils.getRGBForThisHigh(loged);
		}

		return result;
	}

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("Projet PLE");
		JavaSparkContext context = new JavaSparkContext(conf);
		//context.setLogLevel("warn");

		JavaPairRDD<String, PortableDataStream> rddBin=context.binaryFiles(PathDir);
		rddBin=rddBin.filter(file -> {
			FileSystem fs = FileSystem.get(new Configuration());
			FileStatus fileStatus=fs.getFileStatus(new Path(file._1));
			return fileStatus.getModificationTime() < Time.now();
		});
		
		JavaPairRDD<String, short[]> pairRddConvert = rddBin.mapToPair(fileToConvert -> {
			byte [] binary = fileToConvert._2.toArray();
			short [] high = new short[Dem3Size * Dem3Size];

			for(int i =0; i < binary.length; i+=2) {
				byte [] toConvert = new byte[2];
				toConvert [0] = binary[i];
				toConvert [1] = binary[i + 1];
				short converted = (short)(((toConvert[0] & 0xff) << 8) | (toConvert[1] & 0xff));
				high[i/2] = converted;
			}

			return new Tuple2<String, short[]>(fileToConvert._1, high);
		}).mapToPair(fileToRepair -> {
			short tab[] = fileToRepair._2;
			for(int i=0; i<tab.length; ++i){
				if(tab[i]==Short.MIN_VALUE){
					int sum=0;
					int count=0;
					if(i>Dem3Size && tab[i-Dem3Size]!=Short.MIN_VALUE){
						sum+=tab[i-Dem3Size];
						count++;
					}
					if(i<tab.length-Dem3Size && tab[i+Dem3Size]!=Short.MIN_VALUE){
						sum+=tab[i+Dem3Size];
						count++;
					}
					if(i%Dem3Size>0 && tab[i-1]!=Short.MIN_VALUE){
						sum+=tab[i-1];
						count++;
					}
					if(i%Dem3Size<Dem3Size-1 && tab[i+1]!=Short.MIN_VALUE){
						sum+=tab[i+1];
						count++;
					}
					if(count==0){  //safety first
						count=1;
					}
					tab[i]=(short)(sum/count);
				}
				min = (min>tab[i])?tab[i]:min;
				max = (max<tab[i])?tab[i]:max;
			}

			return new Tuple2<String, short[]>(fileToRepair._1, tab);
		});

		pairRddConvert.count();
		StringUtils.generatePalette(min, max);

		JavaPairRDD<String, byte[]> pairRddPNGBinary = pairRddConvert.mapToPair(file -> {
			ByteArrayOutputStream fileCompressed = new ByteArrayOutputStream();
			ImageOutputStream outputStream = ImageIO.createImageOutputStream(fileCompressed);
			
			ImageWriter pngWriter = ImageIO.getImageWritersByFormatName("png").next();
			
			pngWriter.setOutput(outputStream);
			
			BufferedImage img = new BufferedImage(Dem3Size, Dem3Size, BufferedImage.TYPE_INT_RGB);
			img.setRGB(0, 0, Dem3Size, Dem3Size, ConvertHighToRGB(file._2), 0, Dem3Size);

			pngWriter.write(new IIOImage(img, null, null));

			pngWriter.dispose();

			return new Tuple2<String, byte[]>(file._1, fileCompressed.toByteArray());
		});

		//Pour debug
		pairRddPNGBinary.foreach(png -> 	{
			ByteArrayInputStream bis = new ByteArrayInputStream(png._2);
			BufferedImage img = ImageIO.read(bis);
			ImageIO.write(img, "png", new File(StringUtils.extractNameFromPath(png._1) + ".png"));
		});
		
		/*
		pairRddConvert.foreach(file -> {
			BufferedImage img = new BufferedImage(Dem3Size, Dem3Size, BufferedImage.TYPE_INT_RGB);
			img.setRGB(0, 0, Dem3Size, Dem3Size, ConvertHighToRGB(file._2), 0, Dem3Size);
			ImageIO.write(img, "png", new File(StringUtils.extractNameFromPath(file._1) + ".png"));
		});*/

		//pairRddConvert.toDebugString(); peut-être utile, ça affiche la mémoire prise par le rdd

		//C'est okey, ça marche => mettre ça dans une fonction
		/*Connection c;
		try {
			c = ConnectionToHBase.connectTable();
			ConnectionToHBase.createTable(c);
			c.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		JavaPairRDD<ImmutableBytesWritable, Put> pairRddToPut = rddBin.mapToPair(file -> {

			byte[] data = file._2.toArray();
			Put row = ConnectionToHBase.putData(null, file._1, data);
			return new Tuple2<ImmutableBytesWritable, Put>(new ImmutableBytesWritable(), row);
		});

		pairRddToPut.saveAsNewAPIHadoopDataset(ConnectionToHBase.getHBaseConf());*/ 

		//TODO rdd.filter a faire

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
		});*/
	}

}
