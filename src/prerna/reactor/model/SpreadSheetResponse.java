package prerna.reactor.model;

//pojo to send responses for spreadsheet reactors
public class SpreadSheetResponse {
	
	private String titleSheetID;
	private String SheetID;
	private boolean status;
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
	public boolean isStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	}
}
