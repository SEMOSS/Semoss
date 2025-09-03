package prerna.io.connector.google.spreadsheet;

//pojo to send responses for Spreadsheet reactors
public class SpreadSheetResponse {
	
	private String titleSheetID;
	private String sheetID;
	private boolean success;
	public String getTitleSheetID() {
		return titleSheetID;
	}
	public void setTitleSheetID(String titleSheetID) {
		this.titleSheetID = titleSheetID;
	}
	public String getSheetID() {
		return sheetID;
	}
	public void setSheetID(String sheetID) {
		this.sheetID = sheetID;
	}
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
}
