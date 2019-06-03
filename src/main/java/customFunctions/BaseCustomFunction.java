package customFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import parser.actions.ActionException;

public abstract class BaseCustomFunction {
	
	protected String newColName;
	protected String[] colsToApply;

	public BaseCustomFunction() {
		
	}
	
	public void setNewColName(String newColName) {
		this.newColName = newColName;
	}
	
	public void setColsToApply(String[] colsToApply) {
		this.colsToApply = colsToApply;
	}
	
	public abstract  Dataset<Row> execute( Dataset<Row>input);
}
