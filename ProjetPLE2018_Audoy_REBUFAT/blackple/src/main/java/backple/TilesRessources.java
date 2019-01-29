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
		String lo=(lon<0)?"S":"N";
		String lons=(lon<10)?"0"+lon:String.valueOf(lon);
		int lat=Integer.parseInt(y);
		String la=(lat<0)?"E":"W";
		String lats=(lat<100)?"0"+lat:String.valueOf(lat);
		if(lat<10){
			lats="0"+lats;
		}
		byte[] tile=HBasePicker.get(lo+lons+la+lats+"Z"+1);
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
