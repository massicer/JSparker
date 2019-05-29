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
	TAKE("take");

    private String field;

    EnumActionField(String field) {
        this.field = field;
    }

    public String getVal() {
        return field;
    }
}
