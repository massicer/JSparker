package test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.junit.Test;

import parser.GrafterizerParser;
import parser.pipeline.Pipeline;
import pipenineExecutor.PipelineExecutor;

public class PythonCustomFunctionPipelineTest {
	
	
	@Test
	public void testCustomPythonFunction() {
		
		try {
	
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": ["
				+"	{	\"isPreviewed\": false,\n" + 
						"		\"name\": \"python_custom_function\",\n" + 
						"		\"displayName\": \"python_custom_function\",\n" + 
						"\"pythonFunctionName\": \"sum\"," +
						"		\"pythonCode\": \"def sum (a):\\n\\treturn 2\",\n" + 
						"		\"newColName\": \"colFromPython\",\n" + 
						"		\"columnsArray\": [\n" + 
						"		{\n" + 
						"			\"colName\": \"street\",\n" + 
						"			\"colValue\": \"\"\n" + 
						"		}\n" + 
						"		],\n" + 
						"		\"docstring\": \"Execute python script and create nee column\",\n" + 
						"		\"__type\": \"AddColumnsFunction\",\n" + 
						"		\"$$hashKey\": \"object:252\",\n" + 
						"		\"fabIsOpen\": false\n" + 
						"	}"
				+ "],\n" + 
				"		\"__type\": \"Pipeline\"\n" + 
				"	}]\n" + 
				"}";
		
		JSONObject js = null;
		try {
			js = new JSONObject(json);
		}catch(Exception e) {
			e.printStackTrace();
			fail("Excp occurred");
		}
		
		// Is a json so parse it
		GrafterizerParser parser = new GrafterizerParser();
		ArrayList<Pipeline>  pipelineParsed = null;
		try {
			pipelineParsed = parser.parsePipelineJson(js);
		}catch(Exception e) {
			e.printStackTrace();
			fail("Excp during parser occurred");
		}
		
		// create simple Dataframe
		 SparkSession sparkSession = SparkSession.builder()
	                .appName("jsonSparker")
	                .master("local")
	                .getOrCreate();


	        SQLContext sqlContext = sparkSession.sqlContext();
	        Dataset<Row> dataset = sqlContext.read()
	                .option("header", true)
	                .csv("example-data.csv"); //comment option if you dont want an header
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
	    result.show();
	    
		}catch(Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	
}// end test suite
