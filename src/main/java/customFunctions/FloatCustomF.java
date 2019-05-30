package customFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;

public class FloatCustomF extends BaseCustomFunction {

	public FloatCustomF() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> execute(Dataset<Row> input) {
		String newColName = super.newColName;
		
		if (newColName != null && super.colsToApply != null && super.colsToApply.length > 0) {
			input = input.withColumn(newColName, input.col(super.colsToApply[0]).cast(DataTypes.FloatType));
		}
		
		return input;
	}

}
