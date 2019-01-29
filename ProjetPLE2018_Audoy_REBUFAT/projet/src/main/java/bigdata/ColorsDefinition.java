package bigdata;

import java.awt.Color;
import java.io.Serializable;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class ColorsDefinition implements Serializable {

	private static final long serialVersionUID = 1L;
	private final int BARLOWMIDDLE=200;
	private final int BARMIDDLEHIGH=4000;
	private final int BARTOOHIGH=7000;
	private final int NEGHIGH=240;
	private final int LOWHIGH=100;
	private final int MIDDLEHIGH=50;
	private final int HIGH=30;
	private final int TOOHIGH=0;
	private final float SATMAXNORMAL =1.f;
	private final float SATMINNORMAL =.6f;
	private final float BRIMINNORMAL =.3f;
	private final float BRIMAXNORMAL =.5f;
	private final float BRIMINHIGH=0.1f;

	private int [] colors; 
	private int zeroAlt;
	private Broadcast<int []> broadcastedColors;

	private static ColorsDefinition instance;

	private ColorsDefinition() {
		// TODO Auto-generated constructor stub
	}

	public static ColorsDefinition getInstance() {
		if(instance == null)
			instance = new ColorsDefinition();

		return instance;
	}

	public void generatePalette(int min, int max) {
		if(min < 0){
			zeroAlt = Math.abs(min) + 1;
		}else {
			zeroAlt = 0;
		}
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

	public int getRGBForThisHigh(short high) {
		return (high<0)?colors[Math.abs(high)]:colors[high-1 + zeroAlt-1];
	}


	private int augCouleur(int color, int nb, int ite) {
		float all=(SATMAXNORMAL - SATMINNORMAL)+(BRIMAXNORMAL - BRIMINNORMAL);
		float ecart=all/nb;
		float sat=0.f;
		float bri=0.f;
		float limit=100*(BRIMAXNORMAL-BRIMINNORMAL)/all;
		if(color==NEGHIGH){
			sat= SATMAXNORMAL;
			bri= BRIMINNORMAL;
			ecart=(BRIMAXNORMAL-BRIMINNORMAL)/nb;
			bri+=ite*ecart;
		}else if(color==TOOHIGH) {
			sat=0.f;
			ecart=0.6f/nb;
			bri= BRIMAXNORMAL-ite*ecart;
		}else if(color==HIGH) {
			sat=SATMAXNORMAL;
			ecart=(BRIMAXNORMAL-BRIMINHIGH)/nb;
			bri=BRIMAXNORMAL-ite*ecart;
		}else{
			sat= SATMINNORMAL;
			bri= BRIMAXNORMAL;
			if(ite*ecart+sat<SATMAXNORMAL){
				sat+=ite*ecart;
			}else {
				sat = SATMAXNORMAL;
				bri -= ite*ecart;
			}
		}
		return Color.getHSBColor(color/360f, sat, bri).getRGB();
	}

}
