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
	public void testGroupAndAggregate() {
		
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"isPreviewed\": false,\n" + 
				"			\"name\": \"group-rows\",\n" + 
				"			\"displayName\": \"group-rows\",\n" + 
				"			\"colnames\": [{\n" + 
				"				\"id\": 0,\n" + 
				"				\"value\": \"name\"\n" + 
				"			}, {\n" + 
				"				\"id\": 1,\n" + 
				"				\"value\": \"sex\"\n" + 
				"			}],\n" + 
				"			\"colnamesFunctionsSet\": [{\n" + 
				"				\"id\": 2,\n" + 
				"				\"value\": \"age\"\n" + 
				"			}, \"COUNT\"],\n" + 
				"			\"separatorSet\": [null],\n" + 
				"			\"__type\": \"GroupRowsFunction\",\n" + 
				"			\"docstring\": \"Group and aggregate\"\n" + 
				"		}],\n" + 
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
        //dataset.show();
        
        
      
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        result.show();
	}
	
	@Test
	public void testSortDataset() {
		/*
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"name\": \"sort-dataset\",\n" + 
				"			\"displayName\": \"sort-dataset\",\n" + 
				"			\"isPreviewed\": false,\n" + 
				"			\"colnamesSorttypesMap\": [{\n" + 
				"				\"colname\": {\n" + 
				"					\"id\": 0,\n" + 
				"					\"value\": \"name\"\n" + 
				"				},\n" + 
				"				\"sorttype\": \"Alphabetical\",\n" + 
				"				\"order\": false,\n" + 
				"				\"__type\": \"ColnameSorttype\"\n" + 
				"			}, {\n" + 
				"				\"colname\": {\n" + 
				"					\"id\": 0,\n" + 
				"					\"value\": \"age\"\n" + 
				"				},\n" + 
				"				\"sorttype\": \"Numerical\",\n" + 
				"				\"order\": false,\n" + 
				"				\"__type\": \"ColnameSorttype\"\n" + 
				"			}],\n" + 
				"			\"__type\": \"SortDatasetFunction\",\n" + 
				"			\"docstring\": \"Sort column\"\n" + 
				"		}],\n" + 
				"		\"__type\": \"Pipeline\"\n" + 
				"	}]\n" + 
				"}";
		*/
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"name\": \"sort-dataset\",\n" + 
				"			\"displayName\": \"sort-dataset\",\n" + 
				"			\"isPreviewed\": false,\n" + 
				"			\"colnamesSorttypesMap\": [{\n" + 
				"				\"colname\": {\n" + 
				"					\"id\": 0,\n" + 
				"					\"value\": \"age\"\n" + 
				"				},\n" + 
				"				\"sorttype\": \"Numerical\",\n" + 
				"				\"order\": false,\n" + 
				"				\"__type\": \"ColnameSorttype\"\n" + 
				"			}],\n" + 
				"			\"__type\": \"SortDatasetFunction\",\n" + 
				"			\"docstring\": \"Sort column\"\n" + 
				"		}],\n" + 
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
        //dataset.show();
        
        
      
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        result.show();
	}
	
	@Test
	public void testFilterRows() {
		
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"isPreviewed\": false,\n" + 
				"			\"take\": true,\n" + 
				"			\"grepmode\": \"text\",\n" + 
				"			\"colsToFilter\": [{\n" + 
				"				\"id\": 0,\n" + 
				"				\"value\": \"name\"\n" + 
				"			}],\n" + 
				"			\"name\": \"grep\",\n" + 
				"			\"displayName\": \"grep\",\n" + 
				"			\"filterRegex\": null,\n" + 
				"			\"ignoreCase\": true,\n" + 
				"			\"functionsToFilterWith\": [null],\n" + 
				"			\"__type\": \"GrepFunction\",\n" + 
				"			\"docstring\": \"Filter rows\",\n" + 
				"			\"filterText\": \"alice\"\n" + 
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
        //dataset.show();
        
        
      
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        result.show();
	}
	
	@Test
	public void testParseAndExecuteShiftRows() {
		
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"name\": \"shift-row\",\n" + 
				"			\"displayName\": \"shift-row\",\n" + 
				"			\"isPreviewed\": false,\n" + 
				"			\"indexFrom\": 4,\n" + 
				"			\"indexTo\": 0,\n" + 
				"			\"shiftrowmode\": \"position\",\n" + 
				"			\"__type\": \"ShiftRowFunction\",\n" + 
				"			\"docstring\": \"Shift (move) row\"\n" + 
				"		}],\n" + 
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
        //dataset.show();
        
        
      
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        result.show();
	}

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
	public void testParseAndExecuteTakeRows() {
		
		String json = "{\"pipelines\": "
				+ "[\n" + 
				"    {\n" + 
				"    \"functions\": [\n" + 
				"    {\n" + 
				"    \"isPreviewed\": false,\n" + 
				"    \"indexFrom\": 0,\n" + 
				"    \"indexTo\": 1,\n" + 
				"    \"name\": \"drop-rows\",\n" + 
				"    \"displayName\": \"drop-rows\",\n" + 
				"    \"docstring\": \"Drop 2 first row(s)\",\n" + 
				"    \"take\": true,\n" + 
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
	public void testParseAndExecuteAddRow() {
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"name\": \"add-row\",\n" + 
				"			\"displayName\": \"add-row\",\n" + 
				"			\"isPreviewed\": true,\n" + 
				"			\"position\": 0,\n" + 
				"               \"values\": [\n" + 
				"                  \"Davide\",\n" + 
				"                  \"m\",\n" + 
				"                  \"20\",\n" + 
				"                  \"boh\",\n" + 
				"                  \"60\"\n" + 
				"               ],\n" + 
				"			\"__type\": \"AddRowFunction\",\n" + 
				"			\"docstring\": \"Add rows\"\n" + 
				"		}],\n" + 
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
	        
	        
	      
	        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
	        result.show();
	        
	        dataset.show();
	        
	        
	      
	        dataset = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
	        dataset.show();
	        
		}catch(Exception e) {
			fail("Excp occurred");
			e.printStackTrace();
		}
	}

	@Test
	public void testParseAndExecuteMerge2Columns() {
		
		try {
	
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"	\"isPreviewed\": false,\n" + 
				"	\"newColName\": \"nuovoNome\",\n" + 
				"	\"colsToMerge\": [{\n" + 
				"		\"id\": 0,\n" + 
				"		\"value\": \"name\"\n" + 
				"	}, {\n" + 
				"		\"id\": 1,\n" + 
				"		\"value\": \"sex\"\n" + 
				"	}],\n" + 
				"	\"name\": \"merge-columns\",\n" + 
				"	\"displayName\": \"merge-columns\",\n" + 
				"	\"separator\": \"-\",\n" + 
				"	\"__type\": \"MergeColumnsFunction\",\n" + 
				"	\"docstring\": \"Merge columns\"\n" + 
				"}],\n" + 
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
	
	@Test
	public void testParseAndExecuteMerge3Columns() {
		
		try {
	
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"	\"isPreviewed\": false,\n" + 
				"	\"newColName\": \"nuovoNome\",\n" + 
				"	\"colsToMerge\": [{\n" + 
				"		\"id\": 0,\n" + 
				"		\"value\": \"name\"\n" + 
				"	}, {\n" + 
				"		\"id\": 1,\n" + 
				"		\"value\": \"sex\"\n" + 
				"	},"+
				 "{\n" + 
					"		\"id\": 1,\n" + 
					"		\"value\": \"age\"\n" + 
					"	}"
				+ "],\n" + 
				"	\"name\": \"merge-columns\",\n" + 
				"	\"displayName\": \"merge-columns\",\n" + 
				"	\"separator\": \"-\",\n" + 
				"	\"__type\": \"MergeColumnsFunction\",\n" + 
				"	\"docstring\": \"Merge columns\"\n" + 
				"}],\n" + 
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

	

	@Test
	public void setsSplitColumn() {
		
		try {
			
			String json = "{\n" + 
					"	\"pipelines\": [{\n" + 
					"		\"functions\": ["+
					"{\n" + 
					"	\"isPreviewed\": false,\n" + 
					"	\"colName\": {\n" + 
					"		\"id\": 0,\n" + 
					"		\"value\": \"name\"\n" + 
					"	},\n" + 
					"	\"separator\": \"i\",\n" + 
					"	\"name\": \"split\",\n" + 
					"	\"displayName\": \"split\",\n" + 
					"	\"docstring\": \"commenti\",\n" + 
					"	\"__type\": \"SplitFunction\"\n" + 
					"}"
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
