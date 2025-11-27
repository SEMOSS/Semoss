package prerna.logging;

public enum DateRangeMode {
	DAY, WEEK, MONTH, CUSTOM;

    public static DateRangeMode from(String s) {
        if (s == null) return MONTH;
        try { return DateRangeMode.valueOf(s.trim().toUpperCase()); }
        catch (IllegalArgumentException e) { return MONTH; }
    }
}
