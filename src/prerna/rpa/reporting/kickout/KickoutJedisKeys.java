package prerna.rpa.reporting.kickout;

import java.text.SimpleDateFormat;

public class KickoutJedisKeys {
	
	private static final String TIMESTAMPS_POSTFIX = "timestamps";
	private static final String REFERENCE_POSTFIX = "reference";
	private static final String TIMESERIES_POSTFIX = "timeseries";
	private static final String PROCESSED_POSTFIX = "processed";
	private static final String OPEN_RECORDS_POSTFIX = "open_records";
	
	private static final String TIMESTAMP_FORMAT = "yyyy-MM-dd_HH:mm:ss";
	
	private KickoutJedisKeys() {
		throw new IllegalStateException("Constants class");
	}
	
	// TODO docs: javadocs for each of these - what jedis datatypes they represent
	public static String timestampsKey(String prefix) {
		return prefix + "_" + TIMESTAMPS_POSTFIX;
	}
	
	public static String referenceKey(String prefix) {
		return prefix + "_" + REFERENCE_POSTFIX;
	}
	
	public static String timeseriesKey(String prefix) {
		return prefix  + "_" + TIMESERIES_POSTFIX;
	}
	
	public static String processedKey(String prefix) {
		return prefix  + "_" + PROCESSED_POSTFIX;
	}
	
	public static String reportKey(String prefix, String reportTimestamp) {
		return prefix + "_" + reportTimestamp;
	}
	
	public static String openRecordsKey(String prefix) {
		return prefix + "_" + OPEN_RECORDS_POSTFIX;
	}
	
	public static SimpleDateFormat timestampFormatter() {
		return new SimpleDateFormat(TIMESTAMP_FORMAT);
	}
}
