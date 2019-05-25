package server;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
 
@Path("/status")
public class CiaoService {
 
	@GET
	@Path("/health")
	public Response getMsg() {
 
		String output = "System is up!";
 
		return Response.status(200).entity(output).build();
 
	}
 
}