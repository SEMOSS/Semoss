package prerna.engine.impl.model.message;

public enum MessageType {

	// @formatter:off
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
	// @formatter:on

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

	public static boolean isResponseMessage(MessageType type) {
		if (type == RESPONSE_COT || type == RESPONSE_TEXT || type == RESPONSE_TOOL || type == RESPONSE_MEDIA) {
			return true;
		}
		return false;
	}

	public static boolean isInputMessage(MessageType type) {
		return !isResponseMessage(type);
	}
}