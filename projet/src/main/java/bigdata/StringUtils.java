package bigdata;

public final class StringUtils {

	private static final int lowHigh = 0x72b9e3;
	private static final int middleHigh = 0x92ef4e;
	private static final int higher = 0xf4e3c3;
	private static int [] colors; 
	private static int zeroAt;
	
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
	
	public static void generatePalette(int min, int max) {
		if(min < 0)
			zeroAt = Math.abs(min) + 1;
		else 
			zeroAt = 0;
		
		int lowDeg = 0, midDeg = 0, highDeg = 0;
		int size = max + Math.abs(min);
		colors = new int [size];
		
		for(int i = 0; i < size ; ++i) {
			if(i < Math.abs(min)) {
				colors[i] = lowHigh + lowDeg;
				//lowDeg+= 0xFF;
			}else
				if(i < 2000) {
					colors[i] = middleHigh + midDeg;
					//midDeg += 0xFF;
				}else {
					colors[i] = higher + highDeg;
					//highDeg += 0xFF;
				}

		}
	}
	
	public static int getRGBForThisHigh(int high) {
		return colors[high + zeroAt];
	}
}
