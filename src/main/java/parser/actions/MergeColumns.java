package parser.actions;

import java.util.ArrayList;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;

import com.esotericsoftware.minlog.Log.Logger;

import static org.apache.spark.sql.functions.*;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

/*
  {
	"isPreviewed": false,
	"newColName": "nuovoNome",
	"colsToMerge": [{
		"id": 0,
		"value": "a"
	}, {
		"id": 1,
		"value": "b"
	}],
	"name": "merge-columns",
	"displayName": "merge-columns",
	"separator": "separatore",
	"__type": "MergeColumnsFunction",
	"docstring": "Merge columns"
}
 */

public class MergeColumns extends BaseAction{
	
	private static final class SingleMergeColumn{
		private int id;
		private String value;
		
		// Define Json Keys
		private enum MergeKeys{
			ID("id"),VALUE("value");
			
			private String val;
			
			public String getVal() { return this.val;}
			
			MergeKeys(String val){ this.val = val;}
		}
		
		public SingleMergeColumn(JSONObject js) throws ActionException {
			
			if(js == null) throw new ActionException("SingleMergeColumn - js in constructor is null");
			try {
				// 0. ID
				if(js.isNull(MergeKeys.ID.getVal())) throw new ActionException("SingleMergeColumn - ID in json is null");
				this.id = js.getInt(MergeKeys.ID.val);
				
				// 1. value
				if(js.isNull(MergeKeys.VALUE.getVal())) throw new ActionException("SingleMergeColumn - VALUE in json is null");
				
				this.value = js.getString(MergeKeys.VALUE.val);
				
			}catch (Exception e) {
				throw new ActionException(e.getMessage());
			}
		}
	}

	// Attributes
	private String separator;
	private ArrayList<SingleMergeColumn> colsToMerge;
	private String newColName;
	
	public MergeColumns(JSONObject js, int sequenceNumber) throws ActionException{
		super(js,sequenceNumber,ActionType.DEFAULT);
		
		try {
		
			// 0.A check if has cols to merge values
			if(js.isNull(EnumActionField.COLS_TO_MERGE.getVal()))
				throw new ActionException("ColsTOMerge is null in json");
			
			// 0.B initialize colsToMerge array
			this.colsToMerge = new ArrayList<>();
			
			// 0.C extract array and populate colsTOMerge arraylist
			JSONArray colsArray = js.getJSONArray(EnumActionField.COLS_TO_MERGE.getVal());
			for(int i = 0; i<colsArray.length(); i++) {
				this.colsToMerge.add(new SingleMergeColumn(colsArray.getJSONObject(i))); 
			}
			
			// 1. Extract separator
			if(js.isNull(EnumActionField.SEPARATOR.getVal()))
				throw new ActionException("SEPARATOR is null in json");
			this.separator = js.getString(EnumActionField.SEPARATOR.getVal());
			
			// 2. Extract new col name
			if(js.isNull(EnumActionField.NEW_COLUMN_NAME.getVal()))
				throw new ActionException("NEW_COLUMN_NAME is null in json");
			this.newColName = js.getString(EnumActionField.NEW_COLUMN_NAME.getVal());
		
		}catch (Exception e) {
			throw new ActionException(e.getMessage());
		}
	}
	
	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		
		// 0. if cols are <= 1 do nothing
		if( this.colsToMerge.size() <= 1) return input;
		

		// 1. two cols
		if (this.colsToMerge.size() == 2) {
			
			// MErge
			input = input.withColumn(this.newColName, 
					concat(
							input.col(this.colsToMerge.get(0).value),
							lit(this.separator), 
							input.col(this.colsToMerge.get(1).value))
					);
			
			// Remove merged columns
			input = input.drop(input.col(this.colsToMerge.get(0).value)) ;
			input = input.drop(input.col(this.colsToMerge.get(1).value)) ;
			
			return input;
		}
		
		/*
		// 2. More than two cols
		String colsTempToMergeAndRemove = null;
		String lastColumnNameToMergeAndDrop = null;
		
		for(int i = 0; i< this.colsToMerge.size() - 1; i++) {
			
			if ( i < this.colsToMerge.size() - 2) {
				lastColumnNameToMergeAndDrop = "tmp"+i
				// MErge
				input = input.withColumn(this.newColName, 
						concat(
								input.col(this.colsToMerge.get(i).value),
								lit(this.separator), 
								input.col(this.colsToMerge.get(i+1).value))
						);
				
				// Remove merged columns
				input = input.drop(input.col(this.colsToMerge.get(i).value)) ;
				input = input.drop(input.col(this.colsToMerge.get(i+1).value)) ;
			}
		
		}// end for
		
		input = input.withColumn(this.newColName, concat(c));
		
		return input;
		*/
		
		String lastColumnNameAdded = null;
		for(int i = 0; i< this.colsToMerge.size() ; i++) {
			
			if ( i == 0) continue;
			
			if ( i == 1) {
				lastColumnNameAdded = "tmp"+i;
				input = mergeTwoColumnsAndDeleteThem(
						input,
						this.colsToMerge.get(i).value,
						this.colsToMerge.get(i-1).value,
						lastColumnNameAdded);
				
			}else if ( i < this.colsToMerge.size() - 2) {
				String newColNameTMP = "tmp"+i;
				input = mergeTwoColumnsAndDeleteThem(
						input,
						this.colsToMerge.get(i).value,
						lastColumnNameAdded,
						newColNameTMP);
				lastColumnNameAdded = newColNameTMP;
				
			}else {
				input = mergeTwoColumnsAndDeleteThem(input,this.colsToMerge.get(i).value, lastColumnNameAdded,this.newColName );
			}
			
		}
		return input;
		
		
	}
	
	public Dataset<Row> mergeTwoColumnsAndDeleteThem(Dataset<Row> input, String colSource1, String colSource2, String newColName) {
	
		
		// MErge
		input = input.withColumn(newColName, 
				concat(
						input.col(colSource1),
						lit(this.separator), 
						input.col(colSource2)
				));
		
		// Remove merged columns
		input = input.drop(input.col(colSource1)) ;
		input = input.drop(input.col(colSource2)) ;
		return input;
		
	}

	
}
