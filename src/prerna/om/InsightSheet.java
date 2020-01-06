package prerna.om;

public class InsightSheet {

	private String sheetId;
	private String sheetName;
	private String sheetBackground;
	
	public InsightSheet(String sheetId) {
		this.sheetId = sheetId;
	}
	
	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}
	
	public void setSheetBackground(String sheetBackground) {
		this.sheetBackground = sheetBackground;
	}
	
	public String getSheetId() {
		return this.sheetId;
	}
	
	public String getSheetName() {
		return this.sheetName;
	}
	
	public String getSheetBackground() {
		return this.sheetBackground;
	}
}
