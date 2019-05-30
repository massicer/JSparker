package customFunctions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class LowercaseCustomF extends BaseCustomFunction {

	public LowercaseCustomF() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> execute(Dataset<Row> input) {
		String newColName = super.newColName;
		if (newColName != null && super.colsToApply != null && super.colsToApply.length > 0) {
			input = input.withColumn(newColName, lower(col(super.colsToApply[0])));
		}
		return input;
	}

}
