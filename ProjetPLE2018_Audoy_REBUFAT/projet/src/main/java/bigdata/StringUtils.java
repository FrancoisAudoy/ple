package bigdata;

import java.io.Serializable;

@SuppressWarnings("serial")
public final class StringUtils implements Serializable {

	public StringUtils() {}

	public  String extractNameFromPath(String path){
		String [] splitedPath = path.split("/");
		String [] name = splitedPath[splitedPath.length - 1].split("\\.");
		return name[0];	
	}

	public String [] extractLonLat(String path) {
		if(path.contains("/") || path.contains("\\."))
			path = extractNameFromPath(path);

		String [] lonLatparsed = new String[3];
		lonLatparsed[0]=path.substring(1, 3);
		lonLatparsed[1]=path.substring(4,7);

		if(path.contains("Z")) 
			lonLatparsed[2] = getZ(path);

		if (lonLatparsed.length >= 2) {
			return lonLatparsed;
		}else {
			return null;
		}
	}


	public String getZ(String coord) {

		String [] z = coord.split("[a-zA-Z0-9]*Z");

		return z[0];
	}


	public String [] extractXYZ(String location) {

		String [] xyz = location.split("[XYZ]");

		return xyz;
	}


}
