package pythonCustomFunction;



import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.json.JSONArray;
import org.json.JSONObject;
import org.python.core.PyFunction;
import org.python.core.PyString;
import org.python.util.PythonInterpreter;

import customFunctions.BaseCustomFunction;
import parser.actions.ActionException;
import parser.actions.BaseAction;
import parser.actions.BaseAction.ActionType;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class PythonCustomFunction extends BaseAction {
	
	// column class used to store colums into array
		private static class SingleColumn {

			// Key val
			public enum SingleColumnKey {

				COLNAME("colName"), COLVALUE("colValue");

				private final String value;

				SingleColumnKey(String val) {
					this.value = val;
				}
			}

			// Attributes
			private String colName;
			private String colValue;
	

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


		} // end SingleColumn class
	
	/*
	 * See this tutorial for more info and optimization: https://mr-dai.github.io/embedding-jython/
	 */
	
	private static final PythonInterpreter intr = new PythonInterpreter();
    
    private static final String FUNC_TPL = String.join("\n", new String[]{
        "def __call__():",
        "    %s",
        "",
    });
    
    // Attributes
	protected String newColName;
	protected ArrayList<SingleColumn> colsToApply;
    private String pythonCode;
    private final PyFunction func;
    private  String CustomFunctionName = "randomName";
	
    public PythonCustomFunction(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.CUSTOM);
		
		// parse/extract info
		LogManager.getShared().logInfo("PythonCustomFunction -  init called");
		try {
		
			
			// 1. Columns Array
			if(js.isNull(EnumActionField.COLUMNS_ARRAY.getVal())) 
				throw new ActionException("PythonCustomFunction - Columns are missing in json");
			JSONArray colArray = js.getJSONArray(EnumActionField.COLUMNS_ARRAY.getVal());
			
			// 1b For Each item create a column
			this.colsToApply = new ArrayList<>();
			for(int i = 0; i<colArray.length(); i++) {
				this.colsToApply.add(new SingleColumn(colArray.getJSONObject(i)));
			}

		
		
		// Extract new col name
		if (js.isNull(EnumActionField.NEW_COLUMN_NAME.getVal())) {
			LogManager.getShared().logError("PythonCustomFunction - Error while parsing NEW_COLUMN_NAME attribute");
			throw new ActionException("PythonCustomFunction - init - new cols name is missing in json");
		}
		this.newColName = js.getString(EnumActionField.NEW_COLUMN_NAME.getVal());

		// Parse PYTHON_CODE
		if (js.isNull(EnumActionField.PYTHON_CODE.getVal())) {
			LogManager.getShared().logError("PythonCustomFunction - Error while parsing PYTHON_CODE attribute");
			throw new ActionException("PythonCustomFunction - init - PYTHON_CODE is missing in json");
		}
		this.pythonCode = js.getString(EnumActionField.PYTHON_CODE.getVal());
		
    	
		if (js.isNull(EnumActionField.PYTHON_ACTION_NAME.getVal())) {
			LogManager.getShared().logError("PythonCustomFunction - Error while parsing PYTHON_ACTION_NAME attribute");
			throw new ActionException("PythonCustomFunction - init - PYTHON_ACTION_NAME is missing in json");
		}
		CustomFunctionName = js.getString(EnumActionField.PYTHON_ACTION_NAME.getVal());
		
    	// Extract python code
    	intr.exec(pythonCode);
    	LogManager.getShared().logInfo("PythonCustomFunction -  inserted python code");
    	
    	
        func = (PyFunction) intr.get(CustomFunctionName);
        LogManager.getShared().logInfo("PythonCustomFunction -  extracted function");
        
		} catch (Exception e) {
			LogManager.getShared().logError("PythonCustomFunction - Error while parsing  function");
			e.printStackTrace();
			throw new ActionException(e.getMessage());
		}
	}

    @Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		
		switch(this.colsToApply.size()) {
			
			case 1:
				input = executePythonScriptOneParameter(input);
				break;
			default:
				return input;
				//throw new ActionException("Can't apply custom python function with "+this.colsToApply.size()+" parameters");
				
		}
		return input;
				
	}
	
	// Works for only one parameter
	private Dataset<Row> executePythonScriptOneParameter(Dataset<Row> input) {
		
		LogManager.getShared().logInfo("PythonCustomFunction -  executePythonScriptOneParameter - preparing to execute");
		
		Column a = col(this.colsToApply.get(0).colName);
		Object result = func.__call__(new PyString("ciaooo"));
		
		//input = input.withColumn(newColName, col(super.colsToApply[0]));
		LogManager.getShared().logInfo("PythonCustomFunction -  executed with result: "+result + result.toString());
		
		return input;
		
	}

}
