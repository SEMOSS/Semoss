package prerna.reactor.model;

//pojo to send responses for Spreadsheet reactors
public class SpreadSheetResponse {
	
	private String titleSheetID;
	private String SheetID;
	private boolean success;
	public String getTitleSheetID() {
		return titleSheetID;
	}
	public void setTitleSheetID(String titleSheetID) {
		this.titleSheetID = titleSheetID;
	}
	public String getSheetID() {
		return SheetID;
	}
	public void setSheetID(String sheetID) {
		SheetID = sheetID;
	}
	public boolean isSuccess() {
		return success;
	}
	public void setSuccess(boolean success) {
		this.success = success;
	}
}
