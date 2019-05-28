package parser.actions;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.json.JSONObject;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class DropRows extends BaseAction {

    // Attributes
    private int indexFrom;
    private int indexTo;

    public DropRows(JSONObject js, int sequenceNumber) throws ActionException {
        super(js, sequenceNumber, ActionType.DEFAULT);

        // parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Drop Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.DROP_ROWS))
                throw new ActionException("Error while creating Drop Rows function. Name is not conformed");

            // 1. Index From
            if (js.isNull(EnumActionField.INDEX_FROM.getVal()))
                throw new ActionException("Error while creating Drop Rows function. Index From is not present");
            this.indexFrom = js.getInt(EnumActionField.INDEX_FROM.getVal());

            // 2. Index To
            if (js.isNull(EnumActionField.INDEX_TO.getVal()))
                throw new ActionException("Error while creating Drop Rows function. Index To is not present");
            this.indexTo = js.getInt(EnumActionField.INDEX_TO.getVal());

        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing DropRows function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Drop rows function parsed");

    }
    
    @Override
    public Dataset<Row> actionToExecute(Dataset<Row> input) {
    	input = input.withColumn("index",functions.monotonically_increasing_id());
    	int colIndex = input.columns().length - 1;
    	final int iS = indexFrom;
    	final int iE = indexTo;
    	input = input.filter((FilterFunction<Row>) r -> ((r).getLong(colIndex) <= iS || (r).getLong(colIndex) >= iE));
    	
    	input = input.drop(col("index")); // remove column at the end
    	
    	return input;
    }
    @Override
    public boolean equals(Object obj) {
    	
    	if(obj != null &&
    			obj instanceof DropRows) {
    		
    		DropRows copy = (DropRows) obj;
    		
    		return copy.getName().equals(this.getName()) &&
    				copy.indexFrom == this.indexFrom &&
    				copy.indexTo == this.indexTo;
    				
    	}else {
    		return false;
    	}
    			
    }

	public int getIndexFrom() {
		return indexFrom;
	}

	public void setIndexFrom(int indexFrom) {
		this.indexFrom = indexFrom;
	}

	public int getIndexTo() {
		return indexTo;
	}

	public void setIndexTo(int indexTo) {
		this.indexTo = indexTo;
	}
    
    
}
