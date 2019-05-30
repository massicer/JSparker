package parser.actions;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;

import customFunctions.BaseCustomFunction;
import customFunctions.CustomFunctionParser;
import parser.actions.BaseAction.ActionType;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class DeriveColumn extends BaseAction {
	
	BaseCustomFunction[] allCustomFunctions;

	public DeriveColumn(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);

        // parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Drop Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.DERIVE_COLUMN))
                throw new ActionException("Error while creating Derive Col function. Name is not conformed");
            
            if (js.isNull("newColName"))
                throw new ActionException("Error while creating Derive Col function. newColName is not conformed");
            
            String newColName = js.getString("newColName");

            JSONArray colsToApplyJson = js.getJSONArray("colsToDeriveFrom");
            String[] colsToApply = new String[colsToApplyJson.length()];
            for(int i = 0; i < colsToApplyJson.length(); i++) {
            	String colToApplyName = colsToApplyJson.getJSONObject(i).getString("value");
            	colsToApply[i] = colToApplyName;
            }
            
            //TODO: need to parse params - functParams
            
            //custom functions
            JSONArray functionsToDeriveWith = js.getJSONArray("functionsToDeriveWith");
            this.allCustomFunctions = new BaseCustomFunction[functionsToDeriveWith.length()];
            for(int i = 0; i < functionsToDeriveWith.length(); i++) {
            	JSONObject singleFunctionWithJson = functionsToDeriveWith.getJSONObject(i);
            	CustomFunctionParser cFunctionParser = new CustomFunctionParser(singleFunctionWithJson);
            	BaseCustomFunction cFunction = cFunctionParser.parseCustomFunction();
            	cFunction.setNewColName(newColName);
            	cFunction.setColsToApply(colsToApply);
            	this.allCustomFunctions[i] = cFunction;
            }
            

        }catch (Exception e){
        	e.printStackTrace();
            LogManager.getShared().logError("Error while parsing DeriveColumn function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Drop rows function parsed");
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		
		for (int i = 0; i< this.allCustomFunctions.length; i++) {
			input = this.allCustomFunctions[i].execute(input);
		}
		return input;
	}

}
