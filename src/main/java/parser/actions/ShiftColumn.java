package parser.actions;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class ShiftColumn extends BaseAction {
	
	/*
	 * ---------------------
	 FIRST TYPE (Rigth Most)
	 ----------------------
	 {
		"name": "shift-column",
		"displayName": "shift-column",
		"isPreviewed": false,
		"colFrom": {
			"id": 0,
			"value": "sex"
		},
		"indexTo": null,
		"shiftcolmode": "eods",
		"__type": "ShiftColumnFunction",
		"docstring": "Shift (move) column"
	}
	 
	 * ---------------------
	 SECOND TYPE (Move to a selected index)
	 ----------------------
	 {
		"name": "shift-column",
		"displayName": "shift-column",
		"isPreviewed": false,
		"colFrom": {
			"id": 0,
			"value": "name"
		},
		"indexTo": 2,
		"shiftcolmode": "position",
		"__type": "ShiftColumnFunction",
		"docstring": "Shift (move) column"
	}
	 
	 */
	
	private static final class SingleMoveColumn{
		private int id;
		private String value;
		
		// Define Json Keys
		private enum TakeColumnJsonKeys{
			ID("id"),VALUE("value");
			
			private String val;
			
			public String getVal() { return this.val;}
			
			TakeColumnJsonKeys(String val){ this.val = val;}
		}
		
		public SingleMoveColumn(JSONObject js) throws ActionException {
			
			if(js == null) throw new ActionException("SingleMoveColumn - js in constructor is null");
			try {
				// 0. ID
				if(js.isNull(TakeColumnJsonKeys.ID.getVal())) throw new ActionException("SingleMoveColumn - ID in json is null");
				this.id = js.getInt(TakeColumnJsonKeys.ID.val);
				
				// 1. value
				if(js.isNull(TakeColumnJsonKeys.VALUE.getVal())) throw new ActionException("SingleMoveColumn - VALUE in json is null");
				
				this.value = js.getString(TakeColumnJsonKeys.VALUE.val);
				
			}catch (Exception e) {
				throw new ActionException(e.getMessage());
			}
		}
	}
	
	// Attributes
	private SingleMoveColumn colToMove;
	private Mode workingMode = null;
	private Integer positionWhereToMove;
	
	
	// Defines modes
	private enum Mode{
		RIGHTMOST("eods"),
		MOVETOINDEX("position");
		
		private String val;
		
		public String getVal() { return this.val;}
		
		Mode(String val){ this.val = val;}
	}

	
	public ShiftColumn(JSONObject js, int sequenceNumber) throws ActionException{
		super(js,sequenceNumber, ActionType.DEFAULT);
		
		try {
			
			// 0. extract column to move
			if(js.isNull(EnumActionField.COL_FROM.getVal()))
				throw new ActionException("ShiftColumn -  colFrom is missing in json");
			this.colToMove = new SingleMoveColumn(js.getJSONObject(EnumActionField.COL_FROM.getVal()));
			
			// 1.A determinate mode
			if(js.isNull(EnumActionField.SHIFT_COL_MODE.getVal()))
				throw new ActionException("ShiftColumn -  SHIFT_COL_MODE is missing in json");
			String modeString = js.getString(EnumActionField.SHIFT_COL_MODE.getVal());
			
			switch(modeString) {
				case "eods":
					this.workingMode = Mode.RIGHTMOST;
					break;
					
				case "position":
					this.workingMode = Mode.MOVETOINDEX;
					break;
					
				default:
					throw new ActionException("ShiftColumn -  mode is not recognized: "+modeString);
			}
			
			LogManager.getShared().logInfo("ShiftColumn - "+this.workingMode.val +" mode detected" );
			// 1.B if mode is Move TO Position check if there is an index
			if(this.workingMode  == Mode.MOVETOINDEX ) {
				
				if (js.isNull(EnumActionField.INDEX_TO.getVal())) 
				throw new ActionException("ShiftColumn -  working mode is \"move to position\" but index to is not present"); 
				
				positionWhereToMove = js.getInt(EnumActionField.INDEX_TO.getVal());
			}
			
		}catch (Exception e) {
			throw new ActionException(e.getMessage());
		}
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		/*
		 * See this link: https://community.hortonworks.com/questions/42464/spark-dataframes-how-can-i-change-the-order-of-col.html
		 */
		input = this.workingMode == Mode.MOVETOINDEX ? executeMoveColToPosition(input) : executeMoveColToEOF(input);
		return input;
	}
	
	private Dataset<Row> executeMoveColToPosition(Dataset<Row> input) { 
		// Simply do a select
		String[] allCols = input.columns();
		ArrayList<Column> finalColsOrder = new ArrayList<>();
		
		
		for(int i =0; i < allCols.length; i++ ) {
			if(i == this.positionWhereToMove)
				finalColsOrder.add(input.col(this.colToMove.value));
			
			if(!(this.colToMove.value.equals(allCols[i]))){
				finalColsOrder.add(input.col(allCols[i]));
			}
		}
		
		LogManager.getShared().logInfo("ShiftColumn - executeMoveColToPosition - final Order before do select: "+finalColsOrder);
		
		Column [] finalCols = {};
		
		
		for (Column c: finalColsOrder) {
			finalCols = append(finalCols, c);
		}
		
		LogManager.getShared().logInfo("ShiftColumn - executeMoveColToPosition - final Order before do select [Array] : "+finalCols.length);
		input = input.select(finalCols);
		
		return input;

	}
	
	private Dataset<Row> executeMoveColToEOF(Dataset<Row> input) { 
		
		// Simply do a select
		String[] allCols = input.columns();
		ArrayList<Column> finalColsOrder = new ArrayList<>();
		
		
		for(int i =0; i < allCols.length; i++ ) {
			// if it is not the last one
			if(i < (allCols.length - 1) && !(this.colToMove.value.equals(allCols[i])))
				finalColsOrder.add(input.col(allCols[i]));
			else if ( i == (allCols.length - 1)) 
				finalColsOrder.add(input.col(this.colToMove.value));
			
		}
		
		LogManager.getShared().logInfo("ShiftColumn - executeMoveColToEOF - final Order before do select: "+finalColsOrder);
		
		Column [] finalCols = {};
		
		
		for (Column c: finalColsOrder) {
			finalCols = append(finalCols, c);
		}
		
		LogManager.getShared().logInfo("ShiftColumn - executeMoveColToEOF - final Order before do select [Array] : "+finalCols.length);
		input = input.select(finalCols);
		
		return input;
	}
	
	static <T> T[] append(T[] arr, T element) {
	    final int N = arr.length;
	    arr = Arrays.copyOf(arr, N + 1);
	    arr[N] = element;
	    return arr;
	}

}
