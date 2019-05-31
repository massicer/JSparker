package parser.actions;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONObject;
import utility.LogManager;

public class MakeDataset extends BaseAction {

  
    public MakeDataset(JSONObject js, int sequenceNumber) throws ActionException {
        super(js, sequenceNumber, ActionType.DEFAULT);
        LogManager.getShared().logSuccess("MakeDatasetfunction parsed");

    }
    
    @Override
    public Dataset<Row> actionToExecute(Dataset<Row> input) {
    	// Does nothing
    	return input;
    }
    
}
