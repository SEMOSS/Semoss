package prerna.query.querystruct.filters;

public class FilterBooleanVal {

	private enum FILTER_BOOLEAN_MAP_TYPE {FRAME, PANEL};
	
	private FILTER_BOOLEAN_MAP_TYPE type = null;
	private String name = null;
	private boolean filterVal = false;
	
	private FilterBooleanVal() {
		
	}
	
	public static FilterBooleanVal getFrameFilter() {
		FilterBooleanVal map = new FilterBooleanVal();
		map.type = FILTER_BOOLEAN_MAP_TYPE.FRAME;
		return map;
	}
	
	public static FilterBooleanVal getPanelFilter() {
		FilterBooleanVal map = new FilterBooleanVal();
		map.type = FILTER_BOOLEAN_MAP_TYPE.PANEL;
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
