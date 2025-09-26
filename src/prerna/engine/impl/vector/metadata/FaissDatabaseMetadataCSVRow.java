package prerna.engine.impl.vector.metadata;

import prerna.date.SemossDate;

public class FaissDatabaseMetadataCSVRow {

    private String id;              // Unique row identifier
    private String source;
    private String attribute;
    private String strValue;
    private Integer intValue;
    private Number numValue;
    private Boolean boolValue;
    private SemossDate dateValue;
    private SemossDate timestampValue;

    // New constructor with ID as the first parameter
    public FaissDatabaseMetadataCSVRow(
        String id,
        String source,
        String attribute,
        String strValue,
        Number intValue,
        Number numValue,
        Boolean boolValue,
        SemossDate dateValue,
        SemossDate timestampValue
    ) {
        this.id = id;
        this.source = source;
        this.attribute = attribute;
        this.strValue = strValue;
        if (intValue != null) {
            this.intValue = intValue.intValue();
        }
        this.numValue = numValue;
        this.boolValue = boolValue;
        this.dateValue = dateValue;
        this.timestampValue = timestampValue;
    }

    // Getter and setter for ID
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }

    public String getSource() {
        return source;
    }
    public String getAttribute() {
        return attribute;
    }
    public String getStrValue() {
        return strValue;
    }
    public Integer getIntValue() {
        return intValue;
    }
    public Number getNumValue() {
        return numValue;
    }
    public Boolean getBoolValue() {
        return boolValue;
    }
    public SemossDate getDateValue() {
        return dateValue;
    }
    public SemossDate getTimestampValue() {
        return timestampValue;
    }
}
