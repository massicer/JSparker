package parser.actions;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.EnumActionField;

/*
 * ° THERE ARE TWO TYPES OF WAY TO TAKE COLUMNS: by id range and by column name
 * ° take == true MEANS take only that rows, otherwise discard them and take others
 * ------------------
 * FIRST WAY
 * ------------------
 {
		"name": "columns",
		"displayName": "columns",
		"isPreviewed": false,
		"columnsArray": [{
			"id": 0,
			"value": "name"
		}],
		"indexFrom": null,
		"indexTo": null,
		"take": true,
		"__type": "ColumnsFunction",
		"docstring": "Take columns"
	}
	
 * ------------------
 * SECOND WAY
 * ------------------
 	{
		"name": "columns",
		"displayName": "columns",
		"isPreviewed": false,
		"columnsArray": [],
		"indexFrom": 0,
		"indexTo": 0,
		"take": true,
		"__type": "ColumnsFunction",
		"docstring": "Take columns"
	}
 */

public class TakeColumns extends BaseAction {
	
	private static final class SingleTakeColumn{
		private int id;
		private String value;
		
		// Define Json Keys
		private enum TakeColumnJsonKeys{
			ID("id"),VALUE("value");
			
			private String val;
			
			public String getVal() { return this.val;}
			
			TakeColumnJsonKeys(String val){ this.val = val;}
		}
		
		public SingleTakeColumn(JSONObject js) throws ActionException {
			
			if(js == null) throw new ActionException("SingleTakeColumn - js in constructor is null");
			try {
				// 0. ID
				if(js.isNull(TakeColumnJsonKeys.ID.getVal())) throw new ActionException("SingleTakeColumn - ID in json is null");
				this.id = js.getInt(TakeColumnJsonKeys.ID.val);
				
				// 1. value
				if(js.isNull(TakeColumnJsonKeys.VALUE.getVal())) throw new ActionException("SingleTakeColumn - VALUE in json is null");
				
				this.value = js.getString(TakeColumnJsonKeys.VALUE.val);
				
			}catch (Exception e) {
				throw new ActionException(e.getMessage());
			}
		}
	}

	// Attributes
	private Integer indexFrom;
	private Integer indexTo;
	private boolean take;
	private ArrayList<SingleTakeColumn> columsTarget;
	private Mode workingMode = null;
	
	// Defines modes
	private enum Mode{
		FIRST,SECOND
	}
	
	public TakeColumns(JSONObject js, int sequenceNumber) throws ActionException{
		super(js,sequenceNumber, ActionType.DEFAULT);
		
		try {
			
			// 0.A check if is first way
			if( (js.isNull(EnumActionField.INDEX_FROM.getVal()) || js.isNull(EnumActionField.INDEX_TO.getVal())) &&
					!js.isNull(EnumActionField.COLUMNS_ARRAY.getVal())){
				
				// assign mode
				workingMode = Mode.FIRST;
				
				// 0.B Check If is second way
			} else if ((!js.isNull(EnumActionField.INDEX_FROM.getVal()) && !js.isNull(EnumActionField.INDEX_TO.getVal())) &&
					js.isNull(EnumActionField.COLUMNS_ARRAY.getVal())) {
				
				// assign mode
				workingMode = Mode.SECOND;
				
		
			}else {
				// 0.C no way detected
				throw new ActionException("TakeColumns - first and second way of transformation are not defined or in conflict each others");
			}
			
			// 3. In every case Extract take or not take mode
			if( js.isNull(EnumActionField.TAKE.getVal()))
				throw new ActionException("TakeColumns - Take is not present in Json");
			this.take = js.getBoolean(EnumActionField.TAKE.getVal());
			
		}catch (Exception e) {
			throw new ActionException(e.getMessage());
		}
	}
	
	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		// TODO Auto-generated method stub
		return null;
	}
	
	// TAKE ROWS - FIRST WAY
	public Dataset<Row> takeRowsFirstWay(Dataset<Row> input) {
		// TODO Auto-generated method stub
		return null;
	}
	
	// TAKE OTHER ROWS - FIRST WAY
	public Dataset<Row> takeOtherRowsFirstWay(Dataset<Row> input) {
		// TODO Auto-generated method stub
		return null;
	}
	
	// TAKE ROWS - SECOND WAY
	public Dataset<Row> takeRowsSecondWay(Dataset<Row> input) {
		// TODO Auto-generated method stub
		return null;
	}
	
	// TAKE OTHER ROWS - SECOND WAY
	public Dataset<Row> takeOtherRowsSecondWay(Dataset<Row> input) {
		// TODO Auto-generated method stub
		return null;
	}

}
