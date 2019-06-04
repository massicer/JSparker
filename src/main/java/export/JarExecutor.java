package export;

import static org.junit.Assert.fail;

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
		
		// 0. Check if parameters are present
		if(args.length < 2) throw new Exception("Jar executor Ex -  parameters missing ");
		
		// 1.A get the file path
		String inputFilePath = args[0];
		
		// 1.B init spark passing info
		Dataset<Row> dataset = initSparkSession(inputFilePath);
		
		// 2. Get the json path
		String jsonPath = args[1];
		JSONObject transf = readJsonInstruction(jsonPath);
		
		// 3. Parse instruction
		GrafterizerParser parser = new GrafterizerParser();
		ArrayList<Pipeline>  pipelineParsed = parser.parsePipelineJson(transf);
		
		// 4. Apply every instruction
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        result.show();

		
	}
	
	private static JSONObject readJsonInstruction(String resourceName) throws JSONException {
		 

        InputStream is = JarExecutor.class.getResourceAsStream(resourceName);
        if (is == null) {
            throw new NullPointerException("Cannot find resource file " + resourceName);
        }

        JSONTokener tokener = new JSONTokener(is);
        JSONObject object = new JSONObject(tokener);
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
