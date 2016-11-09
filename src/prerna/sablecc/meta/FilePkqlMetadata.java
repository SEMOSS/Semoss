package prerna.sablecc.meta;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.sablecc.PKQLEnum;

public class FilePkqlMetadata extends AbstractPkqlMetadata {

	private String fileLoc;
	private List<String> selectors;
	private Map<String, String> dataMap;
	private List<Map<String, Object>> tableJoin;	
	
	public FilePkqlMetadata() {
		
	}
	
	// start required methods
	
	@Override
	public Map<String, Object> getMetadata() {
		Map<String, Object> meta = new HashMap<String, Object>();
		meta.put("fileLocation", this.fileLoc);
		meta.put("selectors", this.selectors);
		meta.put("dataMap", this.dataMap);
		meta.put("tableJoin", this.tableJoin);
		return meta;
	}
	
	@Override
	public String getExplanation() {
		//TODO:
		return "";
	}
	
	// end required methods
	
	// start metadata specific methods
	// used outside of translation
	
	/**
	 * Will create an equivalent pkql to the data.import(csvFile... but using a
	 * data.import from an engine
	 * @param engineName
	 * @param tableToUse
	 * @return
	 */
	public String generatePkqlOnEngine(String engineName, String tableToUse) {
		StringBuilder pkqlStr = new StringBuilder("data.import(api:");
		pkqlStr.append(engineName);
		pkqlStr.append(".query( [");
		pkqlStr.append("c:").append(tableToUse).append(", ");
		int numSelectors = this.selectors.size();
		for(int i = 0; i < numSelectors; i++) {
			String selector = this.selectors.get(i);
			pkqlStr.append("c:").append(tableToUse).append("__").append(selector);
			if( (i + 1) != numSelectors) {
				pkqlStr.append(", ");
			}
		}
		// close the selectors
		// since csv is flat, close the .query as there are no internal joins
		pkqlStr.append(" ] )");
		if(this.tableJoin != null && !this.tableJoin.isEmpty()) {
			pkqlStr.append(",(");
			
			int numJoins = tableJoin.size();
			for(int i = 0; i < numJoins; i++) {
				Map<String, Object> joinInfo = tableJoin.get(i);
				String fromCol = (String) joinInfo.get(PKQLEnum.FROM_COL);
				String relType = (String) joinInfo.get(PKQLEnum.REL_TYPE);
				String toCol = (String) joinInfo.get(PKQLEnum.TO_COL);

				pkqlStr.append("[c:").append(fromCol).append(", ").append(relType).append(", c:").append(toCol).append("]");

				if( (i + 1) != numJoins) {
					pkqlStr.append(", ");
				}
			}
			
			pkqlStr.append(")");
		}
		
		// close the data.import( );
		pkqlStr.append(");");
		
		return pkqlStr.toString();
	}

	// end metadata specific methods

	// start getter/setters
	
	public void setFileLoc(String fileLoc) {
		this.fileLoc = fileLoc;
	}
	
	public void setSelectors(List<String> selectors) {
		this.selectors = selectors;
	}
	
	public void setDataMap(Map<String, String> dataMap) {
		this.dataMap = dataMap;
	}
	
	public void setTableJoin(List<Map<String, Object>> tableJoin) {
		this.tableJoin = tableJoin;
	}
	
	public String getFileLoc() {
		return this.fileLoc;
	}
	
	public Map<String, String> getDataMap() {
		return this.dataMap;
	}
	
	public List<String> getSelectors() {
		return this.selectors;
	}
}
