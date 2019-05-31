package customFunctions;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

public class TitleizeCustomF extends BaseCustomFunction {
	
	private String clojureCode = "(defn titleize [st] (when (seq st) (let [a (clojure.string/split st (read-string \"#\\\" \\\"\")) c (map clojure.string/capitalize a)]  (->> c (interpose \" \") (apply str) clojure.string/trim))))";

	public TitleizeCustomF() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public Dataset<Row> execute(Dataset<Row> input) {
		final String clCode = "(ns user) " + this.clojureCode;

		UDF1<String, Double> udf = row -> {
			//needed to load clojure.core
			clojure.java.api.Clojure.var("clojure.core", "+");
			//
			clojure.lang.Compiler.load(new java.io.StringReader(clCode));
			clojure.lang.IFn foo1 = clojure.java.api.Clojure.var("user", "titleize");
			Object result1 = foo1.invoke(row);
            return (Double)result1;
        };

        //this is not mandatory
        //input.sparkSession().sparkContext().addJar("/Users/davideceresola/Downloads/clojure/clojure.jar");
        //input.sparkSession().sparkContext().addJar("/Users/davideceresola/Downloads/clojure/spec.alpha-0.2.176.jar");
        input.sqlContext().udf().register("titleize", udf, DataTypes.DoubleType);
        input = input.withColumn(super.newColName, callUDF("titleize", col(super.colsToApply[0])));
		
		
		return input;
	}

}
