package prerna.logging;

public enum AuditLogsDateRangeMode {
	DAY, WEEK, MONTH, CUSTOM;

	public static AuditLogsDateRangeMode from(String s) {
		if (s == null) {
			return MONTH;
		}
		try {
			return AuditLogsDateRangeMode.valueOf(s.trim().toUpperCase());
		} catch (IllegalArgumentException e) {
			return MONTH;
		}
	}
}
