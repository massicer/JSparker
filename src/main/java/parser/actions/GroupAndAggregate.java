package parser.actions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.json.JSONArray;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import utility.LogManager;

public class GroupAndAggregate extends BaseAction {
	
	ArrayList<String>colNames;
	ArrayList<FunctionSet>allFunctions;
	
	private static class FunctionSet {
		public String colName;
		public String function;
	}
	
	private static class FunctionNames {
		public static final String AVG = "AVG";
		public static final String MIN = "MIN";
		public static final String MAX = "MAX";
		public static final String SUM = "SUM";
		public static final String COUNT = "COUNT";
	}

	public GroupAndAggregate(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);
		
		// parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Filter Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.GROUP_AGGREGATE))
                throw new ActionException("Error while creating Filter Rows function. Name is not conformed");
            
            // 1. columns
            if (js.isNull(EnumActionField.COL_NAMES.getVal()))
            	throw new ActionException("Error while creating Filter Rows function. colsToFilter is not conformed");
            this.colNames = new ArrayList<String>();
            JSONArray colsJson = js.getJSONArray(EnumActionField.COL_NAMES.getVal());
            for (int i = 0; i < colsJson.length(); i++) {
            	String colName = colsJson.getJSONObject(i).getString("value");
            	this.colNames.add(colName);
            }
            
            // 2. columns function set
            if (js.isNull(EnumActionField.COL_NAMES_FUNCTION_SET.getVal()))
            	throw new ActionException("Error while creating Filter Rows function. colsToFilter is not conformed");
            this.allFunctions = new ArrayList<FunctionSet>();
            JSONArray functsJson = js.getJSONArray(EnumActionField.COL_NAMES_FUNCTION_SET.getVal());
            for (int i = 0; i < functsJson.length(); i++) {
            	if (i % 2 == 0) { //column object
            		FunctionSet fSet = new FunctionSet();
            		String colName = functsJson.getJSONObject(i).getString("value");
            		fSet.colName = colName;
            		this.allFunctions.add(fSet);
            		
            	} else { //function name
            		this.allFunctions.get(i - 1).function = functsJson.getString(i);
            	}
            }

        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing Filter function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Filter rows function parsed");
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		
		//cast to numerical column affected to function
		for(FunctionSet fSet: this.allFunctions) {
			input = input.withColumn("castedNumerical", input.col(fSet.colName).cast(DataTypes.createDecimalType()));
			input = input.drop(input.col(fSet.colName));
			input = input.withColumnRenamed("castedNumerical", fSet.colName);
		}
		
		ArrayList<Column> cols = new ArrayList<Column>();
		for(String colN: this.colNames) {
			cols.add(input.col(colN));
			LogManager.getShared().logInfo("Entro qui1");
		}
		
		Seq<Column> colSeq = convertListToColumnSeq(cols);
		RelationalGroupedDataset gData = input.groupBy(colSeq);
		
		for(FunctionSet fSet: this.allFunctions) {
			//apply
			if(fSet.function.equals(FunctionNames.AVG)) {
				input = gData.mean(fSet.colName);
			} else if(fSet.function.equals(FunctionNames.MAX)) {
				input = gData.max(fSet.colName);
			} else if(fSet.function.equals(FunctionNames.MIN)) {
				input = gData.min(fSet.colName);
			} else if(fSet.function.equals(FunctionNames.SUM)) {
				input = gData.sum(fSet.colName);
			} else if(fSet.function.equals(FunctionNames.COUNT)) {
				input = gData.count();
			}
		}
		
		return input;
	}
	
	public Seq<Column> convertListToColumnSeq(List<Column> inputColumn) {
	    return JavaConverters.asScalaIteratorConverter(inputColumn.iterator()).asScala().toSeq();
	}

}
