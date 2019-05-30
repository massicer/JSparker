package parser.actions;

import java.util.ArrayList;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class SortDataset extends BaseAction {
	
	private static class SortType {
		public static final String ALPHABETICAL = "Alphabetical";
		public static final String NUMERICAL = "Numerical";
		public static final String BYLENGHT = "By length";
		public static final String BYDATE = "Date";
	}
	
	private static class SingleSort {
		public String colName;
		public String sortType;
		public boolean order;
	}
	
	ArrayList<SingleSort>allSorts;

	public SortDataset(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);
		
		// parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Shift Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.SORT_DATASET))
                throw new ActionException("Error while creating Shift Rows function. Name is not conformed");

            //1. sort options
            allSorts = new ArrayList<SingleSort>();
            JSONArray sortMap = js.getJSONArray(EnumActionField.COLSORTMAP.getVal());
            for (int i = 0; i < sortMap.length(); i++) {
            	JSONObject sMap = sortMap.getJSONObject(i);
            	JSONObject colObj = sMap.getJSONObject(EnumActionField.COL_SORT_NAME.getVal());
            	String colName = colObj.getString(EnumActionField.VALUE.getVal());
            	String sortType = sMap.getString(EnumActionField.SORT_TYPE.getVal());
            	boolean order = sMap.getBoolean(EnumActionField.ORDER.getVal());
            	
            	//build object
            	SingleSort sSort = new SingleSort();
            	sSort.colName = colName;
            	sSort.sortType = sortType;
            	sSort.order = order;
            	
            	allSorts.add(sSort);
            }
             
        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing Shift function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Shift rows function parsed");
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		for(SingleSort sSort: this.allSorts) {
			if(sSort.sortType.equals(SortType.NUMERICAL)) {
				input = input.withColumn("castedNumerical", input.col(sSort.colName).cast(DataTypes.createDecimalType()));
				input = input.drop(input.col(sSort.colName));
				input = input.withColumnRenamed("castedNumerical", sSort.colName);
			}
			
			input = input.sort(asc(sSort.colName));
		}
		return input;
	}

}
