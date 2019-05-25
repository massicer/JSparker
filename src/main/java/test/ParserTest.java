package test;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.google.gson.JsonObject;

import parser.GrafterizerParser;
import parser.actions.DropRows;
import parser.actions.enums.ActionName;
import parser.pipeline.Pipeline;

public class ParserTest {
	
	
	@Test
	public void testHealth() {
		assertTrue(true);
	}
	
	@Test
	public void testParseSimplePipeline() {
		
		String json = "{\n" + 
				"	\"pipelines\": [{\n" + 
				"		\"functions\": [{\n" + 
				"			\"isPreviewed\": false,\n" + 
				"			\"indexFrom\": 0,\n" + 
				"			\"indexTo\": 2,\n" + 
				"			\"name\": \"drop-rows\",\n" + 
				"			\"displayName\": \"drop-rows\",\n" + 
				"			\"docstring\": \"Drop 2 first row(s)\",\n" + 
				"			\"take\": false,\n" + 
				"			\"__type\": \"DropRowsFunction\",\n" + 
				"			\"$$hashKey\": \"object:246\"\n" + 
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
		
		// final test
		assertNotNull(pipelineParsed);
		assertEquals(pipelineParsed.size(),1);
		assertEquals(pipelineParsed.get(0).getActions().size(),1);
		assertEquals(pipelineParsed.get(0).getActions().get(0).getName(),ActionName.DROP_ROWS);
		assertEquals(((DropRows) pipelineParsed.get(0).getActions().get(0)).getIndexFrom(),0);
		assertEquals(((DropRows) pipelineParsed.get(0).getActions().get(0)).getIndexTo(),2);
		
	}
	

}
