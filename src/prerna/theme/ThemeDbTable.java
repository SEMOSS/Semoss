package prerna.theme;

public enum ThemeDbTable {
	ADMIN_THEME("ADMIN_THEME"),
	BLOCKS_TEMPLATE("BLOCKS_TEMPLATE");
	
	private final String themeDbTableName;
	
	private ThemeDbTable(String themeDbTableName) {
		this.themeDbTableName = themeDbTableName;
	}
	
	public String getThemeDbTableName() {
		return this.themeDbTableName;
	}
}
