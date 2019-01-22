package prerna.om;

import java.util.Map;

import prerna.query.querystruct.SelectQueryStruct;

public class ColorByValueRule {

	/*
	 * This class is used to wrap around a QueryStruct + some options 
	 * that augment the current visualization
	 * 
	 * This will probably be changed to allow for more generic operations
	 * once we have other values aside from CBV
	 * 
	 */
	
	private String id;
	private SelectQueryStruct qs;
	private Map<String, Object> options;
	
	public ColorByValueRule(String id, SelectQueryStruct qs, Map<String, Object> options) {
		this.id = id;
		this.qs = qs;
		this.options = options;
	}
	
	public String getId() {
		return this.id;
	}
	
	public SelectQueryStruct getQueryStruct() {
		return this.qs;
	}
	
	public Map<String, Object> getOptions() {
		return this.options;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof ColorByValueRule) {
			if(((ColorByValueRule) o).id.equals(this.id)) {
				return true;
			}
		}
		return false;
	}

}
