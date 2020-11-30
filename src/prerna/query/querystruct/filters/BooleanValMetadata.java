package prerna.query.querystruct.filters;

public class BooleanValMetadata {

	private enum BOOLEAN_TYPE {FRAME, PANEL};
	
	private BOOLEAN_TYPE type = null;
	private String name = null;
	private boolean filterVal = false;
	
	private BooleanValMetadata() {
		
	}
	
	public static BooleanValMetadata getFrameVal() {
		BooleanValMetadata map = new BooleanValMetadata();
		map.type = BOOLEAN_TYPE.FRAME;
		return map;
	}
	
	public static BooleanValMetadata getPanelVal() {
		BooleanValMetadata map = new BooleanValMetadata();
		map.type = BOOLEAN_TYPE.PANEL;
		return map;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public void setFilterVal(boolean filterVal) {
		this.filterVal = filterVal;
	}
	
	public String getName() {
		return this.name;
	}
	
	public boolean getFilterVal() {
		return this.filterVal;
	}
	
	public String getTypeString() {
		return this.type + "";
	}
	
}
