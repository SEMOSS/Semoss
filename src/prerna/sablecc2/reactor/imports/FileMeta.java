package prerna.sablecc2.reactor.imports;

import java.util.List;
import java.util.Map;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.Join;

public class FileMeta {

	public enum FILE_TYPE {CSV, EXCEL};
	
	// the string that created this
	private String pixelString;
	// location of the file
	private String fileLoc;
	// original file
	private String originalFile;
	// the selectors used from the file
	private List<IQuerySelector> selectors;
	// the data types that the user defined for the file
	private Map<String, String> dataMap;
	// any modified headers from the original file
	private Map<String, String> newHeaders;
	// any table joins used
	private List<Join> tableJoin;
	// the type of the file
	private FILE_TYPE type;
	// sheet name is only needed for excel
	private String sheetName;
	// date/timestamp specific format request map
	private Map<String, String> additionalTypes;

	
	/**
	 * Will create an equivalent pixel to the import that was being used 
	 * @param engineName
	 * @param tableToUse
	 * @return
	 */
	public String generatePixelOnEngine(String engineName, String tableToUse) {
		StringBuilder pixelString = new StringBuilder("Database(");
		pixelString.append(engineName);
		pixelString.append(") | Select(");
		pixelString.append(tableToUse).append(", ");
		int numSelectors = this.selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			QueryColumnSelector selector = (QueryColumnSelector) this.selectors.get(i);
			pixelString.append(tableToUse).append("__").append(selector.getColumn());
			if( (i + 1) != numSelectors) {
				pixelString.append(", ");
			}
		}
		// close the selectors
		// since csv is flat, close the .query as there are no internal joins
		pixelString.append(")");
		if(this.tableJoin != null && !this.tableJoin.isEmpty()) {
			pixelString.append(" | Merge(Join(");
			
			int numJoins = tableJoin.size();
			for(int i = 0; i < numJoins; i++) {
				Join joinInfo = tableJoin.get(i);
				String fromCol = (String) joinInfo.getSelector();
				String relType = (String) joinInfo.getJoinType();
				String toCol = (String) joinInfo.getQualifier();

				pixelString.append("(").append(fromCol).append(", ").append(relType).append(", ").append(toCol).append(")");

				if( (i + 1) != numJoins) {
					pixelString.append(", ");
				}
			}
			
			pixelString.append("));");
		} else {
			pixelString.append(" | Import();");
		}
		return pixelString.toString();
	}

	// end metadata specific methods

	// start getter/setters
	
	public void setFileLoc(String fileLoc) {
		this.fileLoc = fileLoc;
	}
	
	public void setSelectors(List<IQuerySelector> list) {
		this.selectors = list;
	}
	
	public void setDataMap(Map<String, String> dataMap) {
		this.dataMap = dataMap;
	}
	
	public void setTableJoin(List<Join> joins) {
		this.tableJoin = joins;
	}
	
	public String getFileLoc() {
		return this.fileLoc;
	}
	
	public Map<String, String> getDataMap() {
		return this.dataMap;
	}
	
	public List<IQuerySelector> getSelectors() {
		return this.selectors;
	}
	
	public FILE_TYPE getType() {
		return this.type;
	}
	
	public void setType(FILE_TYPE type) {
		this.type = type;
	}

	public void setSheetName(String sheetName) {
		this.sheetName = sheetName;
	}
	
	public String getSheetName() {
		return this.sheetName;
	}
	
	public void setNewHeaders(Map<String, String> newHeaders) {
		this.newHeaders = newHeaders;
	}
	
	public Map<String, String> getNewHeaders() {
		return this.newHeaders;
	}
	
	public void setPixelString(String pixelString) {
		this.pixelString = pixelString;
	}
	
	public String getPixelString() {
		return this.pixelString;
	}
	
	public void setOriginalFile(String originalFile) {
		this.originalFile = originalFile;
	}
	
	public String getOriginalFile() {
		return this.originalFile;
	}
	
	public Map<String, String> getAdditionalTypes() {
		return additionalTypes;
	}
	
	public void setAdditionalTypes(Map<String, String> additionalTypes) {
		this.additionalTypes = additionalTypes;
	}
	
}
