package asia4jIntegration;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static org.junit.Assert.*;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import it.unimib.disco.asia.ASIA4J;
import it.unimib.disco.asia.ASIA4JFactory;
import parser.GrafterizerParser;
import parser.pipeline.Pipeline;
import pipenineExecutor.PipelineExecutor;

public class PipelineWIthAsia4JTest {
	
   private static final String asiaEndpoint = "http://localhost:9999/";

    @Rule
    public WireMockRule asiaService = new WireMockRule(9999);

 
	
	@Test
	public void testASIA4J_Action() {
		
		try {
		
		 asiaService.stubFor(get(urlMatching("/reconcile?.*"))
	                .withQueryParam("queries", equalToJson("{\"q0\":{\"query\":\"Berlin\",  \"type\":\"A.ADM1\", \"type_strict\":\"should\"}}"))
	                .withQueryParam("conciliator", equalTo("geonames"))
	                .willReturn(aResponse()
	                        .withStatus(200).withBody("{\n" +
	                                "    \"q0\": {\n" +
	                                "        \"result\": [\n" +
	                                "            {\n" +
	                                "                \"id\": \"2950157\",\n" +
	                                "                \"name\": \"Land Berlin\",\n" +
	                                "                \"type\": [\n" +
	                                "                    {\n" +
	                                "                        \"id\": \"A.ADM1\",\n" +
	                                "                        \"name\": \"A.ADM1\"\n" +
	                                "                    }\n" +
	                                "                ],\n" +
	                                "                \"score\": 36.59111785888672,\n" +
	                                "                \"match\": false\n" +
	                                "            }\n" +
	                                "        ]\n" +
	                                "    }\n" +
	                                "}")));
		
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": ["
				+ "{\n" + 
				"		\"isPreviewed\": false,\n" + 
				"		\"name\": \"asia4-map\",\n" + 
				"		\"displayName\": \"asia4-map\",\n" + 
				"		\"newColName\": \"nuovoNome\",\n" + 
				"		\"columnTarget\": \n" + 
				"		{\n" + 
				"			\"id\": 0,\n" + 
				"			\"value\": \"\"\n" + 
				"		}\n" + 
				"		,\n" + 
				"		\"docstring\": \"Add new column\",\n" + 
				"		\"__type\": \"AddColumnsFunction\",\n" + 
				"		\"$$hashKey\": \"object:252\",\n" + 
				"		\"fabIsOpen\": false\n" + 
				"	}],\n" + 
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
			pipelineParsed = parser.parsePipelineJson(js,asiaEndpoint);
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
                .csv("example-data.csv"); //comment option if you don t want an header
        dataset.show();
        
        
      
        Dataset<Row> result = PipelineExecutor.getShared().executePipeline(pipelineParsed, dataset);
        result.show();
        
		}catch(Exception e) {
			e.printStackTrace();
			fail();
			throw e;
		}
	}
	
	
	
}// end test suite
