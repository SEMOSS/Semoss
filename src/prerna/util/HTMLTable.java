package prerna.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc2.om.task.ITask;

/**
 * Generates HTML for a task or data rows
 *
 */
public class HTMLTable {
	
	protected String tableName = null;
	private List<String> tableHeaders = null;
	private List<List<String>> tableData = new ArrayList<>();
	private String tableStyle = SMSS_TABLE_STYLE;
	private String theadStyle = SMSS_THEAD_STYLE;
	private String thStyle = SMSS_TH_STYLE;
	private String tdStyle = SMSS_TD_STYLE;
	private String tableNameStyle = TABLE_NAME_STYLE;


	// Default SEMOSS table styles
	private static final String SMSS_TABLE_STYLE = "style='border-collapse: collapse; border: 1px solid #d9d9d9; font-family: Arial, Helvetica, sans-serif; width: 100%; max-width: 600px;'";
	private static final String SMSS_THEAD_STYLE = "style='background: #f5f5f5; color: #5c5c5c;'";
	private static final String SMSS_TH_STYLE = "style='border: 1px solid #d9d9d9; padding: 8px;'";
	private static final String SMSS_TD_STYLE = "style='border: 1px solid #d9d9d9; padding: 8px;'";
	private static final String TABLE_NAME_STYLE = "style='font-family: Arial, Helvetica, sans-serif; margin: 0; margin-bottom: 4px; color: #5c5c5c;'";


	public HTMLTable() {

	}
	
	public HTMLTable(String tableName) {
		this.tableName = tableName;
	}

	//////////////////////////////////////////////////////////
	// METHODS TO SET TABLE STYLE
	//////////////////////////////////////////////////////////

	public void setTableStyle(String tableStyle) {
		this.tableStyle = tableStyle;
	}

	public void setTheadStyle(String theadStyle) {
		this.theadStyle = theadStyle;
	}

	public void setThStyle(String thStyle) {
		this.thStyle = thStyle;
	}

	public void setTdStyle(String tdStyle) {
		this.tdStyle = tdStyle;
	}
	
	public void setTableNameStyle(String tableNameStyle) {
		this.tableNameStyle = tableNameStyle;
	}
	/**
	 * Generate table html for a task
	 * 
	 * @param task
	 * @return
	 */
	public String generateHtml(ITask task) {
		StringBuilder sb = new StringBuilder();
		sb.append(getTableNameHtml());
		sb.append("<table ").append(tableStyle).append(">");
		IHeadersDataRow row = null;
		// get headers from the task
		if (task.hasNext()) {
			row = task.next();
			this.tableHeaders = Arrays.asList(row.getHeaders());
			sb.append(getTableHeadersHtml());
		}
		// add data
		sb.append("<tbody>");
		sb.append(addRow(row));
		while (task.hasNext()) {
			sb.append("<tr>");
			row = task.next();
			sb.append(addRow(row));
			sb.append("</tr>");
		}
		sb.append("</tbody>");
		sb.append("</table>");
		return sb.toString();
	}

	//////////////////////////////////////////////////////////
	// Methods to generate a table from user defined values
	// need to set headers and data
	//////////////////////////////////////////////////////////
	public String getTableAsHtml() {
		if (this.tableHeaders == null) {
			throw new IllegalArgumentException("Table headers not set");
		}
		StringBuilder sb = new StringBuilder();
		sb.append(getTableNameHtml());
		sb.append("<table ").append(tableStyle).append(">");
		sb.append(getTableHeadersHtml());
		sb.append(getTableBodyHtml());
		sb.append("</table>");
		return sb.toString();
	}

	public void setTableHeaders(List<String> tableHeader) {
		this.tableHeaders = tableHeader;
	}

	public void addTableData(List<String> tableData) {
		this.tableData.add(tableData);
	}

	public boolean isTableDataEmpty() {
		return this.tableData.isEmpty();
	}

	//////////////////////////////////////////////////////////
	// METHODS TO GENERATE HTML
	//////////////////////////////////////////////////////////

	private String addRow(IHeadersDataRow row) {
		StringBuilder sb = new StringBuilder();
		Object[] values = row.getValues();
		for (int i = 0; i < values.length; i++) {
			Object value = values[i];
			sb.append("<td ").append(tdStyle).append(">");
			sb.append(value);
			sb.append("</td>");
		}
		return sb.toString();
	}

	private String getTableHeadersHtml() {
		StringBuilder sb = new StringBuilder();
		sb.append("<thead ").append(theadStyle).append(">");
		sb.append("<tr>");
		for (String column : this.tableHeaders) {
			sb.append("<th ").append(thStyle).append(">");
			sb.append(column);
			sb.append("</th>");
		}
		sb.append("</tr>");
		sb.append("</thead>");
		return sb.toString();

	}

	private String getTableBodyHtml() {
		StringBuilder sb = new StringBuilder();
		sb.append("<tbody>");
		for (List<String> row : this.tableData) {
			sb.append("<tr>");
			for (String column : row) {
				sb.append("<td ").append(tdStyle).append(">");
				sb.append(column);
				sb.append("</td>");
			}
			sb.append("</tr>");
		}
		sb.append("</tbody>");
		return sb.toString();
	}
	
	private String getTableNameHtml() {
		StringBuilder sb = new StringBuilder();
		if (this.tableName != null) {
			sb.append("<h4 ");
			sb.append(tableNameStyle);
			sb.append(">");
			sb.append(this.tableName);
			sb.append("</h4>");
		}
		return sb.toString();
	}

//	public static void main(String[] args) throws Exception {
//		TestUtilityMethods.loadAll("C:\\Users\\rramirezjimenez\\Documents\\workspace\\Semoss\\RDF_Map.prop");
//		String engineProp = "C:\\Users\\rramirezjimenez\\Documents\\workspace\\Semoss\\db\\movTst__92cf8a9f-c66d-4af1-ad9b-ef7a8f014dbe.smss";
//		RDBMSNativeEngine engine = new RDBMSNativeEngine();
//		engine.open(engineProp);
//		DIHelper.getInstance().setLocalProperty("92cf8a9f-c66d-4af1-ad9b-ef7a8f014dbe", engine);
//		HardSelectQueryStruct qs = new HardSelectQueryStruct();
//		qs.setQuery("Select * From MOVIES LIMIT 10");
//		IRawSelectWrapper it = WrapperManager.getInstance().getRawWrapper(engine, qs);
//
//		ITask task = new BasicIteratorTask(qs, it);
//
//		// VHA style
//		HTMLTable emailTable = new HTMLTable("test:");
//		emailTable.setTableStyle("border='1' cellpadding='8' cellspacing='0' style='border-collapse:collapse; border: 1px solid #cccccc;'");
//		emailTable.setThStyle("style='padding: 8px; font-size: 14px; color: #ffffff; background-color:#003E73; border: 1px solid #cccccc;'");
//		emailTable.setTdStyle("valign='middle' align='left' style='padding: 8px; border: 1px solid #cccccc;'");
//		emailTable.setTableNameStyle("style='margin: 0; margin-bottom: 4px; color: #003E73;'");
//		System.out.println(emailTable.generateHtml(task));
//	}
}
