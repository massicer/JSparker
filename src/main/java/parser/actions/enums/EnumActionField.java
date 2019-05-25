package parser.actions.enums;

public enum EnumActionField {

    /*
    {
        "isPreviewed": false,
        "indexFrom": 0,
        "indexTo": 2,
        "name": "drop-rows",
        "displayName": "drop-rows",
        "docstring": "Drop 2 first row(s)",
        "take": false,
        "__type": "DropRowsFunction",
        "$$hashKey": "object:246"
     */

    // values
    NAME("name"),
    DISPLAY_NAME("displayName"),
    DOCSTRING("docstring"),
    TAKE("take"),
    ISPREVIEWED("isPreviewed"),
    INDEX_FROM("indexFrom"),
    INDEX_TO("indexTo");

    private String field;

    EnumActionField(String field) {
        this.field = field;
    }

    public String getVal() {
        return field;
    }
}
