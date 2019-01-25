package backple;

import javax.ws.rs.*;
import javax.ws.rs.core.Response;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

@Path("/")
public class TilesRessources {

	@GET
	@Path("tiles/{x}/{y}/{z}")
	@Produces("image/png")
	public Response getTile(@PathParam("x") String x, @PathParam("y") String y, @PathParam("z") String z) throws Exception {
		ToolRunner.run(HBaseConfiguration.create(), new HBasePicker(), null);
		byte[] tile=HBasePicker.get("X0Y1006021Z1");
		if(tile!=null){
			return Response.ok(tile).build();
		}
		return Response.ok(tile!=null).build();
	}
}
