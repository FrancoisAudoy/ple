package bigdata;

import java.awt.*;

public final class StringUtils {

	private static final Color NEGHIGHMAX = new Color(0x72b9e3);
	private static final Color NEGHIGHMIN= new Color(0x0000e3);
	private static final Color LOWHIGHMIN = new Color(0x90c46a);
	private static final Color LOWHIGHMAX = new Color(0x00c400);
	private static final Color MIDDLEHIGHMIN=new Color(0xd5b27f);
	private static final Color MIDDLEHIGHMAX=new Color(0xb46b00);
	private static final Color HIGHHIGHMIN = new Color(0xf4e3c3);
	private static final Color HIGHHIGHMAX = new Color(0xeaeaea);
	private static final int BARLOWMIDDLE=2000;
	private static final int BARMIDDLEHIGH=6000;
	private static int [] colors; 
	private static int zeroAlt;
	
	private StringUtils() {}

	public static int colorToRGBInt(Color c){
		int r=c.getRed();
		int g=c.getGreen();
		int b=c.getBlue();
		r=Integer.valueOf(String.valueOf(r), 16);
		r*=0x10000;
		g=Integer.valueOf(String.valueOf(g), 16);
		g*=0x100;
		b=Integer.valueOf(String.valueOf(b), 16);
		return r+g+b;
	}

	public static int augCouleur(Color cmin, Color cmax, int nb, int ite){
		int rmin=cmin.getRed();
		int gmin=cmin.getGreen();
		int bmin=cmin.getBlue();
		int rmax=cmin.getRed();
		int gmax=cmin.getGreen();
		int bmax=cmin.getBlue();
		int rdif=rmax-rmin;
		int gdif=gmax-gmin;
		int bdif=bmax-bmin;
		float rdec=((float)rdif)/nb;
		float gdec=((float)gdif)/nb;
		float bdec=((float)bdif)/nb;
		int nr=(int)(rmin+rdec*ite);
		int ng =(int)(gmin+gdec*ite);
		int nblue=(int)(bmin+bdec*ite);
		Color res=new Color(nr, ng, nblue);
		return colorToRGBInt(res);
	}
	
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
			if(i < Math.abs(min)) {
				colors[i] = augCouleur(NEGHIGHMIN, NEGHIGHMAX, Math.abs(min), i);
			}else
				if(i < zeroAlt+BARLOWMIDDLE) {
					colors[i] = augCouleur(LOWHIGHMIN, LOWHIGHMAX, BARLOWMIDDLE, i);
				}else if(i< zeroAlt+BARMIDDLEHIGH){
					colors[i] = augCouleur(MIDDLEHIGHMIN, MIDDLEHIGHMAX, BARMIDDLEHIGH, i);
				}else{
					colors[i] = augCouleur(HIGHHIGHMIN, HIGHHIGHMAX, max-BARMIDDLEHIGH, i);
				}

		}
	}
	
	public static int getRGBForThisHigh(int high) {
		return (high<0)?colors[Math.abs(high)]:colors[high + zeroAlt];
	}
}
