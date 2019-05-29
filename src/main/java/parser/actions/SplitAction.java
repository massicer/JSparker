package parser.actions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Column;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.EnumActionField;

/*
 {
	"isPreviewed": false,
	"colName": {
		"id": 0,
		"value": "a"
	},
	"separator": ",",
	"name": "split",
	"displayName": "split",
	"docstring": "commenti",
	"__type": "SplitFunction"
}
 */

public class SplitAction extends BaseAction {
	
	// Attributes
	private String separator;
	private String colToSplitName;
	private int colToSplitiD;
	
	public SplitAction(JSONObject js, int sequenceNumber) throws ActionException{
		super(js,sequenceNumber,ActionType.DEFAULT);
		
		try {
			
			// 0. Extract separator
			if (js.isNull(EnumActionField.SEPARATOR.getVal()))
				throw new ActionException("SplitAction - Separator is missing in json");
			this.separator = js.getString(EnumActionField.SEPARATOR.getVal());
			
			// 1. Extract obj col name
			if (js.isNull(EnumActionField.COL_NAME.getVal()))
				throw new ActionException("SplitAction - COL_NAME is missing in json");
			JSONObject colNameJSON = js.getJSONObject(EnumActionField.COL_NAME.getVal());
			
			// 1. A extract id 
			if (colNameJSON.isNull(EnumActionField.ID.getVal()))
				throw new ActionException("SplitAction - COL_NAME-ID is missing in json");
			this.colToSplitiD = colNameJSON.getInt(EnumActionField.ID.getVal());
			
			// 1. B extract colToSplitName 
			if (colNameJSON.isNull(EnumActionField.VALUE.getVal()))
				throw new ActionException("SplitAction - COL_NAME-VALUE is missing in json");
			this.colToSplitName = colNameJSON.getString(EnumActionField.VALUE.getVal());
			
		}catch(Exception e) {
			throw new ActionException(e.getMessage());
		}
		
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {

		Column splitted =  split(col(this.colToSplitName),this.separator);
		
		/*
		 * Is impossible to know in how many items the column has been split;
		 * At the moment is assumed the splits are max 2
		/*
		 * */
	
		input = input.withColumn(this.colToSplitName+"1", splitted.getItem(0));
		input = input.withColumn(this.colToSplitName+"2", splitted.getItem(1));
	
		 // remove splitted colum [optional]
		 //input = input.drop(col(this.colToSplitName));
		
		return input;

		
	}
	

}
