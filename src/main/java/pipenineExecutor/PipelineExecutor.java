package pipenineExecutor;

import java.util.ArrayList;
import utility.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import parser.actions.ActionException;
import parser.actions.BaseAction;
import parser.pipeline.Pipeline;

public class PipelineExecutor {
	
	private static PipelineExecutor shared;
	
	public static PipelineExecutor getShared() {
		if(shared == null) shared = new PipelineExecutor();
		return shared;
	}

	public Dataset<Row> executePipeline( ArrayList<Pipeline> pipelines, Dataset<Row> input){
		
		if( pipelines == null) {
			LogManager.getShared().logError("executePipeline - pipelines are null");
			return input;
		}
		
		if( input == null) {
			LogManager.getShared().logError("executePipeline - inputnull");
			return input;
		}
		
		
		LogManager.getShared().logInfo("executePipeline - Preparing to execute # "+pipelines.size()+ " on dataset");
		
		for(Pipeline pip: pipelines) {
			
			for(BaseAction act: pip.getActions()) {
				
				input = act.actionToExecute(input);
			}
		}
		
		LogManager.getShared().logSuccess("executePipeline - Pipeline(s) executed correctly");
		return input;
	}
}
