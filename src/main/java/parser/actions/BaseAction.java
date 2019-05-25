package parser.actions;

import org.json.JSONObject;

/*
Defines a base action with common attributes and methods
 */
public abstract class BaseAction extends Object{

    // enum
    public enum ActionType{
        DEFAULT,
        CUSTOM
    }

    // Attributes
    private JSONObject jsAssociated;
    private String name;
    private String clojureCode;
    private int sequenceNumber;
    private ActionType type;

    // init
    public BaseAction(JSONObject js, int sequenceNumber, ActionType type) throws ActionException{
        super();
        if(js == null) throw new ActionException("Json input Json is null");

        this.sequenceNumber = sequenceNumber;
        this.type = type;
        this.jsAssociated = js;
    }

    public JSONObject getJsAssociated() {
        return jsAssociated;
    }

    public void setJsAssociated(JSONObject jsAssociated) {
        this.jsAssociated = jsAssociated;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClojureCode() {
        return clojureCode;
    }

    public void setClojureCode(String clojureCode) {
        this.clojureCode = clojureCode;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public String toString() {

        String msg = "";
        msg += "Action info:";
        if(name != null) msg += "\nname: "+this.name;
        msg += "\nsequence number: "+this.sequenceNumber;
        if(clojureCode != null) msg += "\nclojure code: "+this.clojureCode;
        if(jsAssociated != null) msg += "\njsAssociated: "+this.jsAssociated.toString();
        msg += "\ntype: "+ (this.type == ActionType.DEFAULT ? "Default" : "Custom");
        return msg;
    }
}
