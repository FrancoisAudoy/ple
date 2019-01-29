package backple;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.File;

import javax.imageio.ImageIO;
import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

@Path("/")
public class TilesRessources {

	@GET
	@Path("tiles/{z}/{x}/{y}")
	@Produces("image/png")
	public Response getTile(@PathParam("x") String x, @PathParam("y") String y, @PathParam("z") String z) throws Exception {
		ToolRunner.run(HBaseConfiguration.create(), new HBasePicker(), null);
		int lon=Integer.parseInt(x);
		String lo="";
		if(lon<180){
			lo="E";
			lon=180-lon;
		}else{
			lo="W";
		}
		
		String lons=(lon<100)?"0"+lon:String.valueOf(lon);
		if(lon<10){
			lons="0"+lons;
		}
		int lat=Integer.parseInt(y);
		String la="";
		if(lat<90){
			la="N";
			lat=90-lat;
		}else{
			la="S";
		}
		String lats=(lon<10)?"0"+lat:String.valueOf(lat);
		byte[] tile=HBasePicker.get(la+lats+lo+lons+"Z"+1);
		if(tile!=null){
			return Response.ok(tile).build();
		}
		BufferedImage img=ImageIO.read(new File(getClass().getClassLoader().getResource("no.png").getFile()));
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
	    ImageIO.write(img, "png", baos);
	    byte[] imageData = baos.toByteArray();
		return Response.ok(imageData).build();
	}
}
