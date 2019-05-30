package customFunctions;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.initcap;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class CapitalizeCustomF extends BaseCustomFunction {

	public CapitalizeCustomF() {
	}

	@Override
	public Dataset<Row> execute(Dataset<Row> input) {
		String newColName = super.newColName;
		if (newColName != null && super.colsToApply != null && super.colsToApply.length > 0) {
			input = input.withColumn(newColName, initcap(col(super.colsToApply[0])));
		}
		return input;
	}

}
