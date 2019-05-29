package parser.actions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class FilterRows extends BaseAction {
	
	private boolean take;
	private boolean ignoreCase;
	private String filterRegex;
	private String filterText;
	private ArrayList<String> colsToFilter;
	//add function to filter next

	public FilterRows(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);
		
		// parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Filter Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.FILTER_ROWS))
                throw new ActionException("Error while creating Filter Rows function. Name is not conformed");
            
            // 1. take
            if (js.isNull(EnumActionField.TAKE.getVal()))
            	throw new ActionException("Error while creating Filter Rows function. take is not conformed");
            this.take = js.getBoolean(EnumActionField.TAKE.getVal());
            
            // 2. ignoreCase
            if (js.isNull(EnumActionField.IGNORECASE.getVal())) {
            	this.ignoreCase = false;
            } else {
            	this.ignoreCase = js.getBoolean(EnumActionField.IGNORECASE.getVal());
            }
            
            // 3. filterRegex
            if (!js.isNull(EnumActionField.FILTER_REGEX.getVal())) {
            	this.filterRegex = js.getString(EnumActionField.FILTER_REGEX.getVal());
            }
            
            // 4. filter text
            if (!js.isNull(EnumActionField.FILTER_TEXT.getVal())) {
            	this.filterText = js.getString(EnumActionField.FILTER_TEXT.getVal());
            }

            // 5. columns to filter
            if (js.isNull(EnumActionField.COLS_TO_FILTER.getVal()))
            	throw new ActionException("Error while creating Filter Rows function. colsToFilter is not conformed");
            this.colsToFilter = new ArrayList<String>();
            JSONArray colsJson = js.getJSONArray(EnumActionField.COLS_TO_FILTER.getVal());
            for (int i = 0; i < colsJson.length(); i++) {
            	String colName = colsJson.getJSONObject(i).getString("value");
            	this.colsToFilter.add(colName);
            }
            

        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing Filter function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Filter rows function parsed");
		 
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		String[] allColumns = input.columns();
		List<Integer> columnsIndexToFilter = new ArrayList<Integer>();
		for (int i = 0; i < allColumns.length; i++) {
			for (int j = 0; j < this.colsToFilter.size(); j++) {
				if (this.colsToFilter.get(j).equals(allColumns[i])) {
					columnsIndexToFilter.add(j);
					continue;
				}
			}
		}
		
		// filter text mode
		if(this.filterText != null) {
			for(Integer colIndex : columnsIndexToFilter) {
				final String toSearch = new String(this.filterText);
				
				//ignore case
				if (this.ignoreCase) {
					input = input.filter((FilterFunction<Row>) r -> ((r).getString(colIndex).toLowerCase().equals(toSearch.toLowerCase())));
				} else {
					input = input.filter((FilterFunction<Row>) r -> ((r).getString(colIndex).equals(toSearch)));
				}
				
			}
			return input;
		}
		
		// regex mode
		if(this.filterRegex != null) {
			for(Integer colIndex : columnsIndexToFilter) {
				final String toSearch = new String(this.filterRegex);
				
				//ignore case
				if (this.ignoreCase) {
					input = input.filter((FilterFunction<Row>) r -> ((r).getString(colIndex).toLowerCase().matches(toSearch.toLowerCase())));
				} else {
					input = input.filter((FilterFunction<Row>) r -> ((r).getString(colIndex).matches(toSearch)));
				}
			}
			return input;
		}
		
		
		return input;
	}

}
