package parser.actions;

import org.json.JSONObject;
import parser.actions.enums.ActionName;
import parser.actions.enums.EnumActionField;
import utility.LogManager;

public class DropRows extends BaseAction {

    // Attributes
    private int indexFrom;
    private int indexTo;


    public DropRows(JSONObject js, int sequenceNumber) throws ActionException {
        super(js, sequenceNumber, ActionType.DEFAULT);

        // parse/extract info
        try {

            // 0.A Name
            if (!js.isNull(EnumActionField.NAME.getVal()))
                throw new ActionException("Error while creating Drop Rows function. Name is not present");
            super.setName(js.getString(EnumActionField.NAME.getVal()));

            // 0.B Name Conformed
            if (!super.getName().equals(ActionName.DROP_ROWS))
                throw new ActionException("Error while creating Drop Rows function. Name is not conformed");

            // 1. Index From
            if (!js.isNull(EnumActionField.INDEX_FROM.getVal()))
                throw new ActionException("Error while creating Drop Rows function. Index From is not present");
            this.indexFrom = js.getInt(EnumActionField.INDEX_FROM.getVal());

            // 2. Index To
            if (!js.isNull(EnumActionField.INDEX_TO.getVal()))
                throw new ActionException("Error while creating Drop Rows function. Index To is not present");
            this.indexTo = js.getInt(EnumActionField.INDEX_TO.getVal());

        }catch (Exception e){
            LogManager.getShared().logError("Error while parsing DropRows function");
            throw new ActionException(e.getMessage());
        }
        LogManager.getShared().logSuccess("Drop rows function parsed");

    }
}
