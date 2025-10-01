package prerna.engine.impl.model.message;

public enum MessageType {
    INPUT_TEXT("INPUT_TEXT"),
    INPUT_MEDIA("INPUT_MEDIA"),
    INPUT_TOOL_EXEC("INPUT_TOOL_EXEC"),
    USER_IMAGE("USER_IMAGE"),
    USER_TEXT("USER_TEXT"),
    RESPONSE_COT("RESPONSE_COT"),
    RESPONSE_TEXT("RESPONSE_TEXT"),
    RESPONSE_TOOL("RESPONSE_TOOL"),
    RESPONSE_MEDIA("RESPONSE_MEDIA"),
    SYSTEM("SYSTEM");
    
    private final String value;
    
    MessageType(String value) {
        this.value = value;
    }
    
    public String getValue() {
        return value;
    }
    
    public static MessageType fromString(String value) {
        for (MessageType type : values()) {
            if (type.value.equals(value)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unknown value: " + value);
    }
}