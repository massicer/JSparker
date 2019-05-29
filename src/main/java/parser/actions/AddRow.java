package parser.actions;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.json.JSONArray;
import org.json.JSONObject;
import org.spark_project.guava.collect.ImmutableList;

import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class AddRow extends BaseAction {
	
	 private ArrayList<String> rowValues;
	 

	public AddRow(JSONObject js, int sequenceNumber) throws ActionException {
		super(js, sequenceNumber, ActionType.DEFAULT);
		
		// parse/extract info
        try {

            // 0.A Name
            if (js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Add Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.ADD_ROW))
                throw new ActionException("Error while creating Add Rows function. Name is not conformed");

            // 1. Values
            if (js.isNull(EnumActionField.VALUES.getVal()))
                throw new ActionException("Error while creating Add Row function. Values is not present");
            JSONArray arr = js.getJSONArray(EnumActionField.VALUES.getVal());
            rowValues = new ArrayList<String>();
            for(int i = 0; i < arr.length(); i++) {
            	String val = arr.getString(i);
            	rowValues.add(val);
            }


        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing Add row function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Add row function parsed");
	}

	@Override
	public Dataset<Row> actionToExecute(Dataset<Row> input) {
		//create struct type
		LogManager.getShared().logInfo(rowValues.toString());
		
		StructField[] sField = new StructField[rowValues.size()];
		for(int i = 0; i < rowValues.size(); i++) {
			sField[i] = DataTypes.createStructField(input.columns()[i],  DataTypes.StringType, true);
		}
		
		StructType schema = DataTypes.createStructType(sField);
		
		Row r1 = RowFactory.create(rowValues.toArray());
		List<Row> rowList = ImmutableList.of(r1);
		Dataset<Row> newDF = input.sparkSession().createDataFrame(rowList, schema);
		
		input = input.union(newDF);
		return input;
	}

}
