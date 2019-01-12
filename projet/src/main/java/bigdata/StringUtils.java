package bigdata;

import java.awt.*;

public final class StringUtils {


	/*private static final Color NEGHIGHMAX = new Color(0x72b9e3);
	private static final Color NEGHIGHMIN= new Color(0x0000e3);
	private static final Color LOWHIGHMIN = new Color(0x90c46a);
	private static final Color LOWHIGHMAX = new Color(0x00c400);
	private static final Color MIDDLEHIGHMIN=new Color(0xd5b27f);
	private static final Color MIDDLEHIGHMAX=new Color(0xb46b00);
	private static final Color HIGHHIGHMIN = new Color(0xf4e3c3);
	private static final Color HIGHHIGHMAX = new Color(0xeaeaea);*/
	private static final int BARLOWMIDDLE=1000;
	private static final int BARMIDDLEHIGH=4000;
	private static final int BARTOOHIGH=7000;
	private static final int NEGHIGH=240;
	private static final int LOWHIGH=120;
	private static final int MIDDLEHIGH=60;
	private static final int HIGH=30;
	private static final int TOOHIGH=0;
	private static final float SATMIN=1.f;
	private static final float SATMAX=.35f;
	private static final float BRIMIN=.15f;
	private static final float BRIMAX=.5f;


	private static int [] colors; 
	private static int zeroAlt;
	
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
			zeroAlt = Math.abs(min) + 1;
		else 
			zeroAlt = 0;
		int size = max + Math.abs(min);
		colors = new int [size];
		for(int i = 0; i < size ; ++i) {
			if(i < zeroAlt) {
				colors[i] = augCouleur(NEGHIGH, zeroAlt, i);
			}else if(i < zeroAlt+BARLOWMIDDLE) {
				colors[i] = augCouleur(LOWHIGH, BARLOWMIDDLE, i-zeroAlt);
			}else if(i< zeroAlt+BARMIDDLEHIGH){
				colors[i] = augCouleur(MIDDLEHIGH, BARMIDDLEHIGH-BARLOWMIDDLE, i-BARLOWMIDDLE);
			}else if(i< zeroAlt+BARTOOHIGH){
				colors[i] = augCouleur(HIGH, BARTOOHIGH-BARMIDDLEHIGH, i-BARMIDDLEHIGH);
			}else{
				colors[i] = augCouleur(TOOHIGH, max-BARTOOHIGH, i-BARTOOHIGH);
			}

		}
	}

	private static int augCouleur(int color, int nb, int ite) {
		float all=(SATMIN-SATMAX)+(BRIMAX-BRIMIN);
		float ecart=all/nb;
		float sat=0.f;
		float bri=0.f;
		if(color==NEGHIGH){
			sat=SATMIN;
			bri=BRIMIN;
			if(ite<(nb/10)*3.5){
				bri+=ite*ecart;
			}else{
				bri=BRIMAX;
				sat+=ite*ecart;
			}
		}else if(color==TOOHIGH) {
			sat=0.f;
			ecart=0.6f/nb;
			bri=BRIMAX-ite*ecart;
		}else{
			sat=SATMAX;
			bri=BRIMAX;
			if(ite<(nb/10)*5.5){
				sat-=ite*ecart;
			}else{
				sat=SATMIN;
				bri-=ite*ecart;
			}
		}
		return Color.getHSBColor(color, sat, bri).getRGB();
	}

	public static int getRGBForThisHigh(int high) {
		return (high<0)?colors[Math.abs(high)]:colors[high + zeroAlt];
	}
}
