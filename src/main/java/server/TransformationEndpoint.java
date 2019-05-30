package server;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.json.JSONObject;

import parser.GrafterizerParser;
import parser.pipeline.Pipeline;
import pipenineExecutor.PipelineExecutor;
import utility.LogManager;

@Path("/transformation")
public class TransformationEndpoint {

	@GET
	@Path("/status")
	public Response getMsg() {

		String output = "transformation Service is up!";

		return Response.status(200).entity(output).build();

	}

	@POST
	@Path("/new")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response uploadFile(@FormDataParam("file") InputStream uploadedInputStream,
			@FormDataParam("file") FormDataContentDisposition fileDetails, @FormDataParam("pipeline") String pipeline) {

		LogManager.getShared().logInfo("Transformation Endpoint - POST/new - New request submited");
		
		// 0.A Check if params are present
		if (uploadedInputStream == null || fileDetails == null)
			return Response.status(404).entity("file param is required").build();
		
		LogManager.getShared().logInfo("Transformation Endpoint - POST/new - File param is present");
		

		if (pipeline == null || pipeline.equals(""))
			return Response.status(404).entity("pipeline param is required").build();
		
		LogManager.getShared().logInfo("Transformation Endpoint - POST/new - pipeline param is present");

		// 0.B Check if pipeline is a json
		JSONObject pipelineJson = null;
		try {
			pipelineJson = new JSONObject(pipeline);
		} catch (Exception e) {
			LogManager.getShared().logError("Transformation Endpoint - POST/new - pipeline param is not a json, exception: "+e.getMessage());
			return Response.status(404).entity("pipeline param is not a valid JSON").build();
		}
		LogManager.getShared().logInfo("Transformation Endpoint - POST/new - pipeline param is a valid json");

		// 1. Save file
		final String baseLocation = System.getProperty("java.io.tmpdir"); // new File("").getAbsolutePath();
		String fileLocation = baseLocation + fileDetails.getFileName();

		File objFile = new File(fileLocation);
		if (objFile.exists())
			objFile.delete();

		try {
			writeToFile(uploadedInputStream, fileLocation);
		} catch (Exception e) {
			LogManager.getShared().logError("Transformation Endpoint - POST/new - error saving file,  exception: "+e.getMessage());

			return Response.status(404).entity("Error during file upload" + e.getMessage()).build();
		}

		
		// 2. Execute Transformation to file
		String resultFilePath = null;
		try {
			resultFilePath = executeTransformationToFile(fileLocation,pipelineJson);
		} catch (Exception e) {
			LogManager.getShared().logError("Transformation Endpoint - POST/new - error during execution of transformation,  exception: "+e.getMessage());
			return Response.status(404).entity("Error during transformation: " + e.getMessage()).build();
		}
		LogManager.getShared().logSuccess("Transformation Endpoint - POST/new - transformation executed correctly");

	
		// 3. Delete file
		objFile = new File(fileLocation);
		if (objFile.exists())
			objFile.delete();
		LogManager.getShared().logSuccess("Transformation Endpoint - POST/new - old file deleted, at location: "+fileLocation);
		
		
		return Response.status(200).entity(
				"Your transformation has been appplied!"
				+ " At the moment we have problem to retrive the result as csv, "
				+ "but you can find it at this path: "+resultFilePath).build();
	}

	private String executeTransformationToFile(String  filePath, JSONObject pipeline) throws Exception{
		
		// 0. Initialize Spark Session And Load Dataset
		SparkSession sparkSession = SparkSession.builder()
                .appName("jsonSparker")
                .master("local")
                .getOrCreate();


        SQLContext sqlContext = sparkSession.sqlContext();
        Dataset<Row> dataset = sqlContext.read()
                .option("header", true) //comment option if you dont want an header
                .csv(filePath); 
        
        
        
        LogManager.getShared().logInfo("Transformation Endpoint - executeTransformationToFile - dataset before transformation");
        dataset.show();
        
        // 1. Execute transformation
        dataset = executePipelineOnDataset(dataset, pipeline);
        
        LogManager.getShared().logSuccess("Transformation Endpoint - executeTransformationToFile - transformation executed");
        
        LogManager.getShared().logInfo("Transformation Endpoint - executeTransformationToFile - dataset after transformation");
        dataset.show();
        
        
        
        // 2. Save output as csv into file
        final String baseLocation = System.getProperty("java.io.tmpdir"); // new File("").getAbsolutePath();
		String fileLocation = baseLocation + "transformationExecutedFolder";
		//dataset.write().option("header", "true").csv(fileLocation);
		dataset
		.coalesce(1)
		.write()
	    .format("com.databricks.spark.csv")
	    .mode("overwrite")
	    .option("header", "true")
	    .save(fileLocation);
		LogManager.getShared().logSuccess("Transformation Endpoint - executeTransformationToFile - transformation csv file saved at path: "+fileLocation);
		
		
		try {
        	// TODO maybe remove close line
	        sparkSession.close();
        }catch(Exception e) {
        	LogManager.getShared().logWarning("Transformation Endpoint - executeTransformationToFile - error during close spark session: "+e.getMessage());
        }
		
		return fileLocation;
		
        
	}
	
	private Dataset<Row> executePipelineOnDataset(Dataset<Row> input, JSONObject pipeline) throws Exception{
		
		// 0. Extract Action from pipeline JSON
		GrafterizerParser parser = new GrafterizerParser();
		ArrayList<Pipeline>  pipelineParsed = parser.parsePipelineJson(pipeline);
		
		// 1. Execute pipeline on Dataframe
		input = PipelineExecutor.getShared().executePipeline(pipelineParsed, input);
		
		return input;
		
	}
	
	// save uploaded file to new location
	private void writeToFile(InputStream uploadedInputStream, String uploadedFileLocation) throws Exception {

		OutputStream out = new FileOutputStream(new File(uploadedFileLocation));
		int read = 0;
		byte[] bytes = new byte[1024];

		out = new FileOutputStream(new File(uploadedFileLocation));
		while ((read = uploadedInputStream.read(bytes)) != -1) {
			out.write(bytes, 0, read);
		}
		out.flush();
		out.close();

	}

}