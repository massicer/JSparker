package parser.actions.enums;

public enum EnumActionField {

    // values
    NAME("name"),
    DISPLAY_NAME("displayName"),
    DOCSTRING("docstring"),
    ISPREVIEWED("isPreviewed"),
    INDEX_FROM("indexFrom"),
    INDEX_TO("indexTo"),
    COLUMNS_ARRAY("columnsArray"),
    VALUES("values"),
    MAPPINGS("mappings"),
    FAB_IS_OPEN("fabIsOpen"),
	TAKE("take"),
	IGNORECASE("ignoreCase"),
	FILTER_TEXT("filterText"),
	FILTER_REGEX("filterRegex"),
	COLS_TO_FILTER("colsToFilter");
    SEPARATOR("separator"),
    COLS_TO_MERGE("colsToMerge"),
    NEW_COLUMN_NAME("newColName"),
    COL_NAME("colName"),
    ID("id"),
    VALUE("value"),
    FAB_IS_OPEN("fabIsOpen");

    private String field;

    EnumActionField(String field) {
        this.field = field;
    }

    public String getVal() {
        return field;
    }
}
