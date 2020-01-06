package prerna.om;

public class InsightSheet {

	private String sheetId;
	private String sheetLabel;
	private String sheetBackground;
	
	public InsightSheet(String sheetId) {
		this.sheetId = sheetId;
	}
	
	public InsightSheet(String sheetId, String sheetLabel) {
		this.sheetId = sheetId;
		this.sheetLabel = sheetLabel;
	}
	
	public String getSheetId() {
		return this.sheetId;
	}
	
	public void setSheetLabel(String sheetLabel) {
		this.sheetLabel = sheetLabel;
	}
	
	public void setSheetBackground(String sheetBackground) {
		this.sheetBackground = sheetBackground;
	}
	
	public String getSheetLabel() {
		return this.sheetLabel;
	}
	
	public String getSheetBackground() {
		return this.sheetBackground;
	}
}
