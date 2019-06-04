package export;

import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStream;
import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.json.JSONTokener;

import parser.GrafterizerParser;
import parser.pipeline.Pipeline;
import pipenineExecutor.PipelineExecutor;
import utility.LogManager;

import org.json.JSONArray;
import org.json.JSONException;

/*
 * USEFULL RESOURCES
 * 1 - https://mapr.com/products/mapr-sandbox-hadoop/tutorials/spark-tutorial/
 * 2 - https://stackoverflow.com/questions/36024565/how-do-i-pass-program-argument-to-main-function-in-running-spark-submit-with-a-j
 */

public class JarExecutor {
	
	/*
	 * HOW TO USE
	 * 1. Create jar
	 * 2. Run with this command:
	 * 	spark-submit --class export.JarExecutor --master yarn \
  exportedJarName.jar /user/user01/input/alice.txt /user/user01/jsonInput.json
	 */
	
	public static void main (String[] args) throws Exception {
		
		LogManager.getShared().logInfo("JarExecutor - main - starting execution with "+args.length+" parameters");

		// 0. Check if parameters are present
		if(args.length < 2) {
			LogManager.getShared().logError("JarExecutor - main - parameters must be >= 2");
			throw new Exception("Jar executor Ex -  parameters missing - parameters must be >= 2");
		}
		
		// 1.A get the file path
		String inputFilePath = args[0];
		LogManager.getShared().logInfo("JarExecutor - main - input data file path: "+inputFilePath);
		
		// 1.B init spark passing info
		LogManager.getShared().logInfo("JarExecutor - main - preparing to init Spark session");
		Dataset<Row> dataset = initSparkSession(inputFilePath);
		LogManager.getShared().logSuccess("JarExecutor - main -  Spark session initialized");
		
		// 2. Get the json path
		String jsonPath = args[1];
		LogManager.getShared().logInfo("JarExecutor - main - pipeline file path: "+jsonPath);
		LogManager.getShared().logInfo("JarExecutor - main - preparing to parse json");
		JSONObject transf = readJsonInstruction(jsonPath);
		LogManager.getShared().logSuccess("JarExecutor - main - json parsed");
		
		// 3. Parse instruction
		LogManager.getShared().logInfo("JarExecutor - main - preparing to parse pipeline");
		GrafterizerParser parser = new GrafterizerParser();
		ArrayList<Pipeline>  pipelineParsed = parser.parsePipelineJson(transf);
		LogManager.getShared().logSuccess("JarExecutor - main - pipeline parsed");
		
		// 4. Apply every instruction
		LogManager.getShared().logInfo("JarExecutor - main - preparing to execute pipeline");
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        LogManager.getShared().logSuccess("JarExecutor - main - pipeline executed");
        result.show();

		
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
        Dataset<Row> dataset = sqlContext.read()
                .option("header", true)
                .csv(inputCsvPath); //comment option if you dont want an header
       
        return dataset;
	}

}
