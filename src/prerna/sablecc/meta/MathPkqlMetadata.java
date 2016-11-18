package prerna.sablecc.meta;

import java.util.Hashtable;
import java.util.List;
import java.util.Map;

public class MathPkqlMetadata  extends AbstractPkqlMetadata {

	private String procedureName;
	private List<String> columnsOperatedOn;
	private List<String> groupByColumns;
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> values = new Hashtable<String, Object>();
		values.put("procedureName", procedureName);
		if(columnsOperatedOn != null) {
			values.put("columnsOperatedOn", columnsOperatedOn);
		}
	    if(groupByColumns != null && !groupByColumns.isEmpty()) {
			values.put("groupByColumns", groupByColumns);
	    }
		return values;
	}

	@Override
	public String getExplanation() {
		// values that are added to template engine
		String template = "Performed {{procedureName}}";
		if (columnsOperatedOn != null && !columnsOperatedOn.isEmpty()) {
			template += " on {{columnsOperatedOn}}";
		}
		// template gets modified if there are also join columns
		if (groupByColumns != null && !groupByColumns.isEmpty()) {
			// remove period from sentence
			//template = template.substring(0, template.indexOf('.'));
			template += " grouped by {{groupByColumns}}";
		}
		return generateExplaination(template, getMetadata());
	}

	// start getters and setters
	
	public void setProcedureName(String procedureName) {
		this.procedureName = procedureName;
	}
	
	public String getProcedureName() {
		return this.procedureName;
	}

	public List<String> getColumnsOperatedOn() {
		return columnsOperatedOn;
	}

	public void setColumnsOperatedOn(List<String> columnsOperatedOn) {
		this.columnsOperatedOn = columnsOperatedOn;
	}
	
	public List<String> getGroupByColumns() {
		return groupByColumns;
	}

	public void setGroupByColumns(List<String> groupByColumns) {
		this.groupByColumns = groupByColumns;
	}

	
}
