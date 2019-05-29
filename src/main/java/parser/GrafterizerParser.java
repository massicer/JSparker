package parser;

import org.json.JSONArray;
import org.json.JSONObject;

import parser.actions.AddColumns;
import parser.actions.AddRow;
import parser.actions.BaseAction;
import parser.actions.DropRows;
import parser.actions.FilterRows;
import parser.actions.RenameColumns;
import parser.actions.ShiftColumn;
import parser.actions.ShiftRow;
import parser.actions.MergeColumns;
import parser.actions.SplitAction;
import parser.actions.TakeColumns;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import parser.pipeline.GrafterizerParserException;
import parser.pipeline.Pipeline;
import utility.LogManager;
import java.util.ArrayList;

public class GrafterizerParser {


    private static GrafterizerParser shared;
    public static GrafterizerParser getShared(){
        if(shared == null) shared = new GrafterizerParser();
        return shared;
    }

    // ENUM
    public enum PipelineField {

        // values
        CUSTOM_FUNCTION_DECLARATIONS_ARRAY("customFunctionDeclarations"),
        PIPELINES("pipelines"),
        FUNCTIONS("functions");

        private String field;

        PipelineField(String field) {
            this.field = field;
        }

        public String getVal() {
            return field;
        }
    }

    // Pipeline could be more than one
    public ArrayList<Pipeline> parsePipelineJson(JSONObject js) throws GrafterizerParserException {
        LogManager.getShared().logInfo("GrafterizerParser - parsePipelineJson() - Preparing to parse pipeline");

        try {

            // 0. Check if json is null
            if (js == null) {
                throw new GrafterizerParserException("GrafterizerParser - parsePipelineJson() - input json is null");
            }

            // 1. extract pipelines
            JSONArray pipelineArray;
            if (js.isNull(PipelineField.PIPELINES.getVal())) {
                throw new GrafterizerParserException("GrafterizerParser - parsePipelineJson() - pipeline(s) field is missing in json");
            }
            pipelineArray = js.getJSONArray(PipelineField.PIPELINES.getVal());
            LogManager.getShared().logSuccess("GrafterizerParser - parsePipelineJson() - Pipeline json array extracted");

            // 1. B
            ArrayList<Pipeline> pipelineParsed = new ArrayList<>();
            LogManager.getShared().logInfo("GrafterizerParser - parsePipelineJson() - Preparing to extract every pipeline");
            pipelineParsed = parseEachFunctionInEachPipeline(pipelineArray);
            LogManager.getShared().logSuccess("GrafterizerParser - parsePipelineJson() - Pipeline(s) extracted");

            // 1. C return pipelines
            return pipelineParsed;

        }catch (Exception e){
            LogManager.getShared().logError("GrafterizerParser - parsePipelineJson() - msg: "+e.getMessage());
            throw new GrafterizerParserException(e.getMessage());
        }
    }

    public ArrayList<Pipeline> parseEachFunctionInEachPipeline(JSONArray jA) throws Exception {

        LogManager.getShared().logInfo("GrafterizerParser - parseEachFunctionInEachPipeline() - Preparing to parse each pipeline obj");
        ArrayList<Pipeline> pipelines = new ArrayList<>();

        if(jA == null){
            throw new GrafterizerParserException("parseEachFunctionInEachPipeline -  array is null");
        }

        for(int i = 0; i<jA.length(); i++){


            LogManager.getShared().logInfo(
                    "GrafterizerParser - parseEachFunctionInEachPipeline() - Preparing to parse pipeline at index # "+i);
            // Create tmp pipeline
            pipelines.add(new Pipeline());

            // extract function element
            JSONArray functionObj = jA.getJSONObject(i).getJSONArray(PipelineField.FUNCTIONS.getVal());
            LogManager.getShared().logInfo(
                    "GrafterizerParser - parseEachFunctionInEachPipeline() - function obj of index # "+i+ " "+functionObj+ " with length of: "+functionObj.length());

            // extract each actions
            for(int j = 0; j<functionObj.length(); j++){

                LogManager.getShared().logInfo(
                        "GrafterizerParser - parseEachFunctionInEachPipeline() - Preparing to parse function at index # "+j +" of pipeline at index: "+i);

                // parse and add action to the current pipelines
                pipelines.get(i).addAction(parseAction((functionObj).getJSONObject(j), j));
            }

        }


        return pipelines;

    }

    public BaseAction parseAction(JSONObject actJs, int progressNumber) throws Exception {

        LogManager.getShared().logInfo(
                "GrafterizerParser - parseAction() - Preparing to parse single action");

        // 0. extract name
        final String nameExtracted = actJs.getString(EnumActionField.NAME.getVal());

        LogManager.getShared().logInfo(
                "GrafterizerParser - parseAction() - single action name extracted: "+nameExtracted);

        // 1. According name create the action
        switch (nameExtracted){
        
            case ActionName.DROP_ROWS :
                LogManager.getShared().logInfo("GrafterizerParser - parseAction() - drop rows action detected");
                return new DropRows(actJs, progressNumber);

            case ActionName.ADD_COLUMNS :
                LogManager.getShared().logInfo("GrafterizerParser - parseAction() - add columns action detected");
                return new AddColumns(actJs, progressNumber);
            
            case ActionName.ADD_ROW:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - add row action detected");
            	return new AddRow(actJs, progressNumber);
            	
            case ActionName.SHIFT_ROW:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - shift row action detected");
            	return new ShiftRow(actJs, progressNumber);
            	
            case ActionName.FILTER_ROWS:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - shift row action detected");
            	return new FilterRows(actJs, progressNumber);

            
            case ActionName.RENAME_COLUMNS:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - rename columns action detected");
                return new RenameColumns(actJs, progressNumber);
                
            case ActionName.MERGE_COLUMNS:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - MERGE columns action detected");
                return new MergeColumns(actJs, progressNumber);
                
            case ActionName.SPLIT:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - SPLIT  action detected");
                return new SplitAction(actJs, progressNumber);
                
            case ActionName.TAKE_COLUMNS:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - TAKE_COLUMNS  action detected");
                return new TakeColumns(actJs, progressNumber);
            	
            case ActionName.SHIFT_COLUMN:
            	LogManager.getShared().logInfo("GrafterizerParser - parseAction() - SHIFT_COLUMN  action detected");
                return new ShiftColumn(actJs, progressNumber);
                
            default:
                LogManager.getShared().logError("GrafterizerParser - parseAction() -  action NOT detected");
                return null;
        }
    }
}
