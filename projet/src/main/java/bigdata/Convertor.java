package bigdata;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;

import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriter;
import javax.imageio.stream.ImageOutputStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

public class Convertor {

	public JavaPairRDD<String, short[]> convertHgtToHigh(JavaPairRDD<String, PortableDataStream> pairRddToConvert, int widthSize, int heightSize){
		return pairRddToConvert.mapToPair(fileToConvert -> {
			byte [] binary = fileToConvert._2.toArray();
			short [] high = new short[widthSize * heightSize];

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
				if(tab[i] == Short.MIN_VALUE){
					int sum = 0;
					int count = 0;
					if(i > widthSize && tab[i-widthSize] != Short.MIN_VALUE){
						sum+=tab[i-widthSize];
						count++;
					}
					if(i<tab.length-widthSize && tab[i+widthSize] != Short.MIN_VALUE){
						sum+=tab[i+widthSize];
						count++;
					}
					if(i%widthSize > 0 && tab[i-1] != Short.MIN_VALUE){
						sum+=tab[i-1];
						count++;
					}
					if(i%widthSize < widthSize - 1 && tab[i+1]!=Short.MIN_VALUE){
						sum+=tab[i+1];
						count++;
					}
					if(count==0){  //safety first
						count=1;
					}
					tab[i]=(short)(sum/count);
				}
			}

			return new Tuple2<String, short[]>(fileToRepair._1, tab);
		}); 
		}

	public JavaPairRDD<String, byte[]> convertToPNG(JavaPairRDD<String, short[]> pairRddToConvert, int widthSize, int heightSize){
		return pairRddToConvert.mapToPair(file -> {
			ByteArrayOutputStream fileCompressed = new ByteArrayOutputStream();
			ImageOutputStream outputStream = ImageIO.createImageOutputStream(fileCompressed);

			ImageWriter pngWriter = ImageIO.getImageWritersByFormatName("png").next();

			pngWriter.setOutput(outputStream);

			BufferedImage img = new BufferedImage(widthSize, heightSize, BufferedImage.TYPE_INT_RGB);
			img.setRGB(0, 0, widthSize, heightSize, ConvertHighToRGB(file._2), 0, widthSize);

			pngWriter.write(new IIOImage(img, null, null));

			pngWriter.dispose();

			return new Tuple2<String, byte[]>(file._1, fileCompressed.toByteArray());
		});
	}
	
	private static int[] ConvertHighToRGB(short [] high) {
		int [] result = new int [high.length];

		for(int i = 0; i < high.length; ++i) {
			short loged = high[i]; //(int) Math.log(high[i]) / (int) Math.log(MaxHigh);
			result[i] = StringUtils.getRGBForThisHigh(loged);
		}

		return result;
	}
}
