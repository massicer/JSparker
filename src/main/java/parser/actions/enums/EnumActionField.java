package parser.actions.enums;

public enum EnumActionField {

    // values
    NAME("name"),
    DISPLAY_NAME("displayName"),
    DOCSTRING("docstring"),
    TAKE("take"),
    ISPREVIEWED("isPreviewed"),
    INDEX_FROM("indexFrom"),
    INDEX_TO("indexTo"),
    COLUMNS_ARRAY("columnsArray"),
    VALUES("values"),
    MAPPINGS("mappings"),
    SEPARATOR("separator"),
    COLS_TO_MERGE("colsToMerge"),
    NEW_COLUMN_NAME("newColName"),
    FAB_IS_OPEN("fabIsOpen");

    private String field;

    EnumActionField(String field) {
        this.field = field;
    }

    public String getVal() {
        return field;
    }
}
