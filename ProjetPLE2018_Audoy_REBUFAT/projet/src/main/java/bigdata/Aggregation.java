package bigdata;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.input.PortableDataStream;

import scala.Tuple2;

public class Aggregation implements Serializable{

	private  int maxZoomLevel;

	private StringUtils strUtils = new StringUtils();

	public  void setMaxZoom(int maxZoom) {
		maxZoomLevel = maxZoom;
	}

	public  JavaPairRDD<String, PortableDataStream> setAggregation(JavaPairRDD<String, PortableDataStream> pairRddToOrganize) {

		return pairRddToOrganize.flatMapToPair(file -> {

			ArrayList<Tuple2<String, PortableDataStream>> linked = new ArrayList<>();


			for(int i = 0; i < maxZoomLevel ; ++i) {
				linked.add(new Tuple2<String, PortableDataStream>(AggregateTo(file._1, i), file._2));
			}

			return linked.iterator();
		});

	}

	private  String AggregateLevel1(String latLon) {
		String [] strLatLon = strUtils.extractLonLat(latLon);

		Integer [] intLatLon = new Integer[2];

		intLatLon[0] = Integer.parseInt(strLatLon[0]);
		intLatLon[1] = Integer.parseInt(strLatLon[1]);

		if( intLatLon[0] % 2 != 0)
			intLatLon[0]--;

		if( intLatLon[1] % 2 != 0)
			intLatLon[1]--;

		StringBuilder finalString = new StringBuilder();

		if(latLon.contains("n") || latLon.contains("N"))
			finalString.append("N");
		else
			finalString.append("S");

		finalString.append(intLatLon[0]);

		if(latLon.contains("w") || latLon.contains("W"))
			finalString.append("W");
		else
			finalString.append("E");

		finalString.append(intLatLon[1]);

		finalString.append("Z1");

		return finalString.toString();
	}

	private  String AggregateTo(String latLong, int level) {

		String strLatLong = strUtils.extractNameFromPath(latLong);

		if(level == 0) {
			StringBuilder finalString = new StringBuilder(strLatLong.toUpperCase()); 
			finalString.append("Z1");
			return finalString.toString();
		}


		if(level == 1){
			return AggregateLevel1(strLatLong);
		}

		String [] strLatLon = strUtils.extractLonLat(latLong);
		Integer [] intLatLon = new Integer [2];

		intLatLon[0] = Integer.parseInt(strLatLon[0]);
		intLatLon[1] = Integer.parseInt(strLatLon[1]);
		if(level %2 == 0) {
			if(intLatLon[0] % 10 == 0 || intLatLon[0] % 10 == 4 ||
					intLatLon[0] % 10 == 8)
				intLatLon[0] -= level;

			if(intLatLon[1] % 10 == 0 || intLatLon[1] % 10 == 4 ||
					intLatLon[1] % 10 == 8)
				intLatLon[1] -= level;
		}
		else {
			if(intLatLon[0] % 10 == 2 || intLatLon[0] % 10 == 6)
				intLatLon[0] -= level;

			if(intLatLon[1] % 10 == 2 || intLatLon[1] % 10 == 6 )
				intLatLon[1] -= level;
		}

		StringBuilder finalString = new StringBuilder();

		if(strLatLong.contains("n") || strLatLong.contains("N"))
			finalString.append("N");
		else
			finalString.append("S");

		finalString.append(intLatLon[0]);

		if(strLatLong.contains("w") || strLatLong.contains("W"))
			finalString.append("W");
		else
			finalString.append("E");

		finalString.append(intLatLon[1]);

		finalString.append("Z");
		finalString.append(level);


		return finalString.toString();
	}

}
