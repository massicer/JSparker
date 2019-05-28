package parser.actions;

import java.util.ArrayList;

import org.apache.commons.lang3.SystemUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class RenameColumns extends BaseAction {
	
	/*
	{
		"isPreviewed": false,
		"name": "rename-columns",
		"displayName": "rename-columns",
		"mappings": [{
			"id": 1,
			"value": "b"
		}, "newName", {
			"id": 2,
			"value": "c"
		}, "newNme2"],
		"functionsToRenameWith": [null],
		"__type": "RenameColumnsFunction",
		"docstring": "Rename columns"
	}
	 */
	
	private static class Mapping{
		
		private static ArrayList<Mapping> getMappingFromJsonArray(JSONArray arr) throws ActionException{
			
			ArrayList<Mapping> maps = new ArrayList<>();
			try {
			
				
				final int maxIndex = arr.length() / 2;
				int currentIndex = 0;
				
				for(int i = 0; i < maxIndex; i++) {
					currentIndex += i;
					
					Mapping singleMap = new Mapping();
					singleMap.id = arr.getJSONObject(currentIndex).getInt(MappingjsonKey.ID.val);
					singleMap.oldName = arr.getJSONObject(currentIndex).getString(MappingjsonKey.VALUE.val);
					currentIndex += 1;
					singleMap.newName = arr.getString(currentIndex);
					
					// add to collection
					maps.add(singleMap);
					
				} // end for
				
			}catch (Exception e) {
				throw new ActionException(e.getMessage());
			}
			
			return maps;
		}
		
		private int id;
		private String oldName;
		private String newName;
		
		private enum MappingjsonKey{
			
			ID("id"),
			VALUE("value");
			
			private String val;
			MappingjsonKey(String val){
				this.val = val;
			}
			public String getVal() { return this.val;}
		}
		

	}

	
	private ArrayList<Mapping> mapping;
	
	public RenameColumns(JSONObject js, int sequenceNumber) throws ActionException{
		super(js,sequenceNumber, ActionType.DEFAULT);
		
		// 0. check if has mappings
		if(js.isNull(EnumActionField.MAPPINGS.getVal())) {
			throw new ActionException("RenameColumns- Mapping is missing in json");
		}
		
		try {
			this.mapping = Mapping.getMappingFromJsonArray(js.getJSONArray(EnumActionField.MAPPINGS.getVal()));
		}catch (Exception e) {
			throw new ActionException(e.getMessage());
		}
	}
	
	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		
		for(Mapping singleMap: this.mapping) {
			LogManager.getShared().logInfo("Single mapping in consideration: "+singleMap.oldName + " "+ singleMap.newName );
			input = input.withColumnRenamed(singleMap.oldName, singleMap.newName);
		}
	    return input;
	}

}
