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

public class PipelineTest {

	@Test
	public void testParseAndExecuteDropRows() {
		
		String json = "{\"pipelines\": "
				+ "[\n" + 
				"    {\n" + 
				"    \"functions\": [\n" + 
				"    {\n" + 
				"    \"isPreviewed\": false,\n" + 
				"    \"indexFrom\": 0,\n" + 
				"    \"indexTo\": 2,\n" + 
				"    \"name\": \"drop-rows\",\n" + 
				"    \"displayName\": \"drop-rows\",\n" + 
				"    \"docstring\": \"Drop 2 first row(s)\",\n" + 
				"    \"take\": false,\n" + 
				"    \"__type\": \"DropRowsFunction\",\n" + 
				"    \"$$hashKey\": \"object:246\"\n" + 
				"    } " + 
				"]\n" + 
				"    }]}";
		
		
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
        //dataset.show();
        
        
      
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        result.show();
	}

	@Test
	public void testParseAndExecuteAddColumns() {
		
		String json = "{\"pipelines\": [\n" + 
				"    {\n" + 
				"    \"functions\": [\n" + 
				"    {\n" + 
				"		\"isPreviewed\": false,\n" + 
				"		\"name\": \"add-columns\",\n" + 
				"		\"displayName\": \"add-columns\",\n" + 
				"		\"columnsArray\": [\n" + 
				"		{\n" + 
				"			\"colName\": \"id\",\n" + 
				"			\"colValue\": \"\",\n" + 
				"			\"specValue\": \"Row number\",\n" + 
				"			\"expression\": null,\n" + 
				"			\"__type\": \"NewColumnSpec\"\n" + 
				"		}\n" + 
				"		],\n" + 
				"		\"docstring\": \"Add new column\",\n" + 
				"		\"__type\": \"AddColumnsFunction\",\n" + 
				"		\"$$hashKey\": \"object:252\",\n" + 
				"		\"fabIsOpen\": false\n" + 
				"	}\n" + 
				"]\n" + 
				"    }]}";
		
		
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
	        
	}

	@Test
	public void testParseAndExecuteRenameColumns() {
		
		try {
		
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"isPreviewed\": false,\n" + 
				"			\"name\": \"rename-columns\",\n" + 
				"			\"displayName\": \"rename-columns\",\n" + 
				"			\"mappings\": [{\n" + 
				"				\"id\": 1,\n" + 
				"				\"value\": \"name\"\n" + 
				"			}, \"new\", {\n" + 
				"				\"id\": 2,\n" + 
				"				\"value\": \"sex\"\n" + 
				"			}, \"newme\"],\n" + 
				"			\"functionsToRenameWith\": [null],\n" + 
				"			\"__type\": \"RenameColumnsFunction\",\n" + 
				"			\"docstring\": \"Rename columns\"\n" + 
				"		}]\n" + 
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
	        dataset.show();
	        
	        
	      
	        PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
	        dataset.show();
	        
		}catch(Exception e) {
			fail("Excp occurred");
			e.printStackTrace();
		}
	}
}
