package customFunctions;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class IntegerLiteralCustomF extends BaseCustomFunction {
	
	private String clojureCode = "(defn integer-literal [s] (if (nil? (re-matches #\"[0-9.]+\" s)) 0 (Integer/parseInt s)))";

	public IntegerLiteralCustomF() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> execute(Dataset<Row> input) {
		final String clCode = "(ns user) " + this.clojureCode;

		UDF1<String, Integer> udf = row -> {
			//needed to load clojure.core
			clojure.java.api.Clojure.var("clojure.core", "+");
			//
			clojure.lang.Compiler.load(new java.io.StringReader(clCode));
			clojure.lang.IFn foo1 = clojure.java.api.Clojure.var("user", "integer-literal");
			Object result1 = foo1.invoke(row);
            return (Integer)result1;
        };

        //this is not mandatory
        //input.sparkSession().sparkContext().addJar("/Users/davideceresola/Downloads/clojure/clojure.jar");
        //input.sparkSession().sparkContext().addJar("/Users/davideceresola/Downloads/clojure/spec.alpha-0.2.176.jar");
        input.sqlContext().udf().register("integer-literal", udf, DataTypes.IntegerType);
        input = input.withColumn(super.newColName, callUDF("integer-literal", col(super.colsToApply[0])));
		
		
		return input;
	}

}
