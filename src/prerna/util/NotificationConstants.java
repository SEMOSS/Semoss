package prerna.util;

public final class NotificationConstants {

	private NotificationConstants() {
	}

	// app catalog key
	public static final String APP_CATALOG = "APP";

	public static final class Priority {
		public static final String HIGH = "HIGH";
		public static final String MEDIUM = "MEDIUM";
		public static final String LOW = "LOW";
	}

	public static final class Type {
		public static final String USER_REQUEST = "USER_REQUEST";
		public static final String USER_ADDITION = "USER_ADDITION";
		public static final String REQUEST_APPROVAL = "REQUEST_APPROVAL";
		public static final String PERMISSION_CHANGE = "PERMISSION_CHANGE";
		public static final String REQUEST_DENIAL = "REQUEST_DENIAL";
		public static final String SMSS_UPDATE = "SMSS_UPDATE";
	}

}
