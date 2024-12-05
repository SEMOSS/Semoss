package prerna.theme;

public enum ThemeDbTable {
	ADMIN_THEME("ADMIN_THEME", "ADMIN_THEME__"),
	BLOCKS_TEMPLATE("BLOCKS_TEMPLATE", "BLOCKS_TEMPLATE__");
	
	private final String themeDbTableName;
	private final String themeDbTablePrefix;
	
	private ThemeDbTable(String themeDbTableName,
			String themeDbTablePrefix) {
		this.themeDbTableName = themeDbTableName;
		this.themeDbTablePrefix = themeDbTablePrefix;
	}
	
	public String getThemeDbTableName() {
		return themeDbTableName;
	}

	public String getThemeDbTablePrefix() {
		return themeDbTablePrefix;
	}
	
}
