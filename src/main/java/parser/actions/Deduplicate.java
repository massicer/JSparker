package parser.actions;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class Deduplicate extends BaseAction {
	
	ArrayList<String>colNames; //if this parameters if null -> full mode enabled

	public Deduplicate(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);
		
		// parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Deduplicate Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.DEDUPLICATE))
                throw new ActionException("Error while creating Deduplicate Rows function. Name is not conformed");
            
            // 1. columns if is in column mode
            if (js.isNull(EnumActionField.MODE.getVal()))
            	 throw new ActionException("Error while creating Deduplicate Rows function. Mode is not conformed");
            
            if(!js.getString(EnumActionField.MODE.getVal()).equals("full")) {
            	if (js.isNull(EnumActionField.COL_DEDUPLICATE_NAME.getVal()))
                	throw new ActionException("Error while creating Deduplicate Rows function. colsToFilter is not conformed");
                this.colNames = new ArrayList<String>();
                JSONArray colsJson = js.getJSONArray(EnumActionField.COL_DEDUPLICATE_NAME.getVal());
                for (int i = 0; i < colsJson.length(); i++) {
                	String colName = colsJson.getJSONObject(i).getString("value");
                	this.colNames.add(colName);
                }
            }
            
        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing Deduplicate function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Deduplicate rows function parsed");
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		if (this.colNames == null) { //full mode enabled
			return input.dropDuplicates();
		}
		
		//remove duplicates only in this columns
		//Convert to string array
        String[] colNamesArray = this.colNames.toArray(new String[this.colNames.size()]);
		input = input.dropDuplicates(colNamesArray);
		return input;
	}

}
