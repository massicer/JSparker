package parser.actions;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class ShiftRow extends BaseAction {
	
	private int indexFrom;
    private int indexTo; // if -1 -> shift to end

	public ShiftRow(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);
		
		// parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Shift Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.SHIFT_ROW))
                throw new ActionException("Error while creating Shift Rows function. Name is not conformed");

            // 1. Index From
            if (js.isNull(EnumActionField.INDEX_FROM.getVal()))
                throw new ActionException("Error while creating Shift Rows function. Index From is not present");
            this.indexFrom = js.getInt(EnumActionField.INDEX_FROM.getVal());

            // 2. Index To
            if (js.isNull(EnumActionField.INDEX_TO.getVal())) {
            	this.indexTo = -1;
            } else {
            	this.indexTo = js.getInt(EnumActionField.INDEX_TO.getVal());
            }
             
        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing Shift function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Shift rows function parsed");
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		input = input.withColumn("index",functions.monotonically_increasing_id());
    	int colIndex = input.columns().length - 1;
    	final int iFrom = indexFrom;
    	final int iTo = indexTo;
    	
    	//filter first-last rows to exclude the selected one
    	Dataset<Row> selectedRow = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) == iFrom));
    	
    	//check where to move row
    	if (iTo == -1) { // move at the end
    		Dataset<Row> filteredFirst = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) < iFrom));
        	Dataset<Row> filteredEnd = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) > iFrom));
        	return filteredFirst.union(filteredEnd).union(selectedRow);
    	}
    	
    	if (iTo < iFrom) {
    		Dataset<Row> filteredFirst = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) < iTo));
    		Dataset<Row> filteredEnd = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) >= iTo && (r).getLong(colIndex) != iFrom));
    		return filteredFirst.union(selectedRow).union(filteredEnd);
    	}
    	
    	if (iTo > iFrom) {
    		Dataset<Row> filteredFirst = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) < iFrom));
    		Dataset<Row> filteredSec = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) > iFrom && (r).getLong(colIndex) <= iTo));
    		Dataset<Row> filteredLast = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) > iTo));
    		return filteredFirst.union(filteredSec).union(selectedRow).union(filteredLast);
    	}
    	
    	
		return input;
	}

}
