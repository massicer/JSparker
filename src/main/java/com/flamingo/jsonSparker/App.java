package com.flamingo.jsonSparker;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;

import org.apache.spark.sql.types.DataTypes;
import org.json.JSONObject;
import scala.collection.Seq;
import sun.rmi.runtime.Log;
import utility.LogManager;

import static org.apache.spark.sql.functions.*;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        SparkSession sparkSession = SparkSession.builder()
                .appName("jsonSparker")
                .master("local")
                .getOrCreate();


        SQLContext sqlContext = sparkSession.sqlContext();
        Dataset<Row> dataset = sqlContext.read()
                .option("header", true)
                .csv("example-data.csv"); //comment option if you dont want an header
        dataset.show();

        //dataset = dataset.filter(new ColumnName("sex").equalTo("f"));
        //dataset.show();

        UDF1<String, String> udf = row -> {
            if (row.equals("f")) return "female";
            return row;
        };

        sqlContext.udf().register("toFemale", udf, DataTypes.StringType);
        dataset = dataset.withColumn("sex2", callUDF("toFemale", col("sex")));

        dataset= dataset.filter(col("sex2").equalTo("female"));

        dataset.show();


        //Dataset<Row> dataset2 = dataset.filter(col.equalTo("f")).withColumn("sex2", "female");
        // dataset.map(mapFunction);

        LogManager.getShared().logInfo("INFOOO");
        LogManager.getShared().logError("ERRORRR");
        LogManager.getShared().logWarning("WARNING");
        LogManager.getShared().logSuccess("SUCCESS");
    }
}
