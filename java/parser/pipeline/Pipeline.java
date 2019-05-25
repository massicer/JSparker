package parser.pipeline;

import parser.actions.BaseAction;

import java.util.ArrayList;

public class Pipeline extends Object {

    //attributes
    private ArrayList<BaseAction> actions; // defines the transformations
    // todo add custom function declarations

    public Pipeline(ArrayList<BaseAction> actions){
        super();
        if(actions != null) this.actions = actions;
    }

    public Pipeline(){
        this.actions = new ArrayList<>();
    }

    // add one more action
    public void addAction(BaseAction act){
        if(act != null){
            if(this.actions == null) this.actions = new ArrayList<>();
            this.actions.add(act);
        }
    }

    public void deleteAllActions(){
        this.actions.clear();
    }

    //Getter/setter
    public ArrayList<BaseAction> getActions() {
        return actions;
    }

    public void setActions(ArrayList<BaseAction> actions) {
        this.actions = actions;
    }
}
