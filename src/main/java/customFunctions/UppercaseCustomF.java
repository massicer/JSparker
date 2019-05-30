package customFunctions;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class UppercaseCustomF extends BaseCustomFunction {

	public UppercaseCustomF() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> execute(Dataset<Row> input) {
		String newColName = super.newColName;
		if (newColName != null && super.colsToApply != null && super.colsToApply.length > 0) {
			input = input.withColumn(newColName, upper(col(super.colsToApply[0])));
		}
		return input;
	}

}
