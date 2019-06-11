package export;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.*;
import org.apache.spark.sql.types.StructType;
import org.json.JSONException;
import org.json.JSONObject;

import parser.GrafterizerParser;
import parser.pipeline.Pipeline;
import pipenineExecutor.PipelineExecutor;
import utility.LogManager;

public class SparkStreaming {

public static void main (String[] args) throws Exception {
		
		LogManager.getShared().logInfo("JarExecutor - main - starting execution with "+args.length+" parameters");

		// 0. Check if parameters are present
		/*
		if(args.length < 2) {
			LogManager.getShared().logError("JarExecutor - main - parameters must be >= 2");
			throw new Exception("Jar executor Ex -  parameters missing - parameters must be >= 2");
		}*/
		
		// 1.A get the file path
		String inputFilePath = "example-data.csv"; //args[0];
		LogManager.getShared().logInfo("JarExecutor - main - input data file path: "+inputFilePath);
		
		// 1.B init spark passing info
		LogManager.getShared().logInfo("JarExecutor - main - preparing to init Spark session");
		Dataset<Row> dataset = initSparkSession(inputFilePath);
		LogManager.getShared().logSuccess("JarExecutor - main -  Spark session initialized");
		
		// 2. Get the json path
		String jsonPath = "ExamplePipeline.json"; //args[1];
		LogManager.getShared().logInfo("JarExecutor - main - pipeline file path: "+jsonPath);
		LogManager.getShared().logInfo("JarExecutor - main - preparing to parse json");
		JSONObject transf = readJsonInstruction(jsonPath);
		LogManager.getShared().logSuccess("JarExecutor - main - json parsed");
		
		// 2.B get asia backend url
		String asiaBackendUrl = null;
		if(args.length >= 3 ) {
			asiaBackendUrl = args[2];
			LogManager.getShared().logInfo("JarExecutor - main - asiaBackend url: "+asiaBackendUrl);
		}else {
			LogManager.getShared().logWarning("JarExecutor - main - asiaBackend url is missing. This may cause a problem if your pipeline cointains some asia4 map action");
		}
		
		// 3. Parse instruction
		LogManager.getShared().logInfo("JarExecutor - main - preparing to parse pipeline");
		GrafterizerParser parser = new GrafterizerParser();
		ArrayList<Pipeline>  pipelineParsed = parser.parsePipelineJson(transf,asiaBackendUrl);
		LogManager.getShared().logSuccess("JarExecutor - main - pipeline parsed");
		
		// 4. Apply every instruction
		LogManager.getShared().logInfo("JarExecutor - main - preparing to execute pipeline");
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        LogManager.getShared().logSuccess("JarExecutor - main - pipeline executed");
        //result.show();

        StreamingQuery q = result
                .writeStream()
                .format("console")
                .start();
        q.awaitTermination();
        
		
	}
	
	private static JSONObject readJsonInstruction(String resourceName) throws JSONException {
		
        String result = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(resourceName));
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null) {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        } catch(Exception e) {
            e.printStackTrace();
        }

        JSONObject object = new JSONObject(result);
        return object;
 

	}
	
	private static Dataset<Row> initSparkSession(String inputCsvPath) {
		SparkSession sparkSession = SparkSession.builder()
                .appName("jsonSparker")
                .master("local")
                .getOrCreate();


        SQLContext sqlContext = sparkSession.sqlContext();
        
        // to get schema
        // !!!! - read the actual schema; This shouldn't be too expensive as Spark's
        // laziness would avoid actually reading the entire file 
        StructType sType = sqlContext.read()
                .option("header", true)
                .csv(inputCsvPath)
                .schema();
        
        Dataset<Row> dataset = sqlContext
        .readStream()
        .option("header", true)
        .schema(sType)
        .csv(inputCsvPath);
       
        return dataset;
	}

}
