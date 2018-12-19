package bigdata;

public final class StringUtils {

	private StringUtils() {}
	
	public static String extractNameFromPath(String path){
		String [] splitedPath = path.split("/");
		String [] name = splitedPath[splitedPath.length - 1].split("\\.");
		return name[0];	
	}

	public static String [] extractLonLat(String path) {
		if(path.contains("/") || path.contains("\\."))
			path = extractNameFromPath(path);

		String [] lonLatparsed = new String[2];
		lonLatparsed[0]=path.substring(1, 3);
		lonLatparsed[1]=path.substring(4,7);//path.split("SsNnWwEe");
		if (lonLatparsed.length == 2)
			return lonLatparsed;
		else 
			return null;
	}
	
}
