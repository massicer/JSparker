package parser.actions;

import java.util.ArrayList;

import org.json.JSONArray;
import org.json.JSONObject;

import parser.actions.BaseAction.ActionType;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

/*
 * {
		"isPreviewed": false,
		"name": "add-columns",
		"displayName": "add-columns",
		"columnsArray": [
		{
			"colName": "id",
			"colValue": "",
			"specValue": "Row number",
			"expression": null,
			"__type": "NewColumnSpec"
		}
		],
		"docstring": "Add new column",
		"__type": "AddColumnsFunction",
		"$$hashKey": "object:252",
		"fabIsOpen": false
	}
 */

public class AddColumns extends BaseAction {

	// column class used to store colums into array
	private static class SingleColumn {

		// Key val
		public enum SingleColumnKey {

			COLNAME("colName"), COLVALUE("colValue"), SPEVALUE("specValue"), EXPRESSION("expression"), TYPE("__type");

			private final String value;

			SingleColumnKey(String val) {
				this.value = val;
			}
		}

		// Attributes
		private String colName;
		private String colValue;
		private String specValue;
		private String expression;
		private String type;

		public SingleColumn(JSONObject js) throws ActionException {
			try {

				// 0. Check JS value
				if (js == null)
					throw new ActionException("Json is null");

				// 1. Col Name - Not Optional
				if (js.isNull(SingleColumnKey.COLNAME.value))
					throw new ActionException("Name is not present");
				this.colName = js.getString(SingleColumnKey.COLNAME.value);

				// 2. Col Value - Optional
				if (!js.isNull(SingleColumnKey.COLVALUE.value)) {
					this.colValue = js.getString(SingleColumnKey.COLVALUE.value);
				}

				// 3. Spec Value - Optional
				if (!js.isNull(SingleColumnKey.SPEVALUE.value)) {
					this.specValue = js.getString(SingleColumnKey.SPEVALUE.value);
				}

				// 4. Expression - Optional
				if (!js.isNull(SingleColumnKey.EXPRESSION.value)) {
					this.expression = js.getString(SingleColumnKey.EXPRESSION.value);
				}

				// 5. Type - Optional
				if (!js.isNull(SingleColumnKey.TYPE.value)) {
					this.type = js.getString(SingleColumnKey.TYPE.value);
				}

			} catch (Exception e) {
				throw new ActionException(e.getMessage());
			}

		}// end constructor SingleColumn

		// Getters/setters

		public String getColName() {
			return colName;
		}

		public void setColName(String colName) {
			this.colName = colName;
		}

		public String getColValue() {
			return colValue;
		}

		public void setColValue(String colValue) {
			this.colValue = colValue;
		}

		public String getSpecValue() {
			return specValue;
		}

		public void setSpecValue(String specValue) {
			this.specValue = specValue;
		}

		public String getExpression() {
			return expression;
		}

		public void setExpression(String expression) {
			this.expression = expression;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

	} // end SingleColumn class

	// Attributes
	private String docString;
	private boolean fabIsOpen;
	private ArrayList<SingleColumn> columns;

	// Constructor
	public AddColumns(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);

		// parse/extract info
		try {
			
			// LogManager.getShared().logInfo("AddColumns - json given: "+js);
			
			// 0. Fab Is Open
			if(js.isNull(EnumActionField.FAB_IS_OPEN.getVal())) throw new ActionException("FabIsOpen is missing");
			this.fabIsOpen = js.getBoolean(EnumActionField.FAB_IS_OPEN.getVal());
			
			// 1. Columns Array
			if(js.isNull(EnumActionField.COLUMNS_ARRAY.getVal())) throw new ActionException("Columns are missing");
			JSONArray colArray = js.getJSONArray(EnumActionField.COLUMNS_ARRAY.getVal());
			
			// 1b For Each item create a column
			this.columns = new ArrayList<>();
			for(int i = 0; i<colArray.length(); i++) {
				this.columns.add(new SingleColumn(colArray.getJSONObject(i)));
			}

		} catch (Exception e) {
			LogManager.getShared().logError("Error while parsing AddColumns function");
			throw new ActionException(e.getMessage());
		}
		LogManager.getShared().logSuccess("AddColumns function parsed");
	}

	// Getters-Setters
	public String getDocString() {
		return docString;
	}

	public void setDocString(String docString) {
		this.docString = docString;
	}

	public boolean isFabIsOpen() {
		return fabIsOpen;
	}

	public void setFabIsOpen(boolean fabIsOpen) {
		this.fabIsOpen = fabIsOpen;
	}

	public ArrayList<SingleColumn> getColumns() {
		return columns;
	}

	public void setColumns(ArrayList<SingleColumn> columns) {
		this.columns = columns;
	}

}
