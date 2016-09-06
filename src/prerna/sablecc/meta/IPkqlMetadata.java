package prerna.sablecc.meta;

import java.util.Map;

import prerna.ui.components.playsheets.datamakers.PKQLTransformation;

public interface IPkqlMetadata {

	String PKQL_NAME = "PKQL_NAME";
	
	/**
	 * Returns the relevant information of the pkql
	 * @return
	 */
	Map<String, Object> getMetadata();
	
	String getExplanation();
	
	void setInvokingPkqlTransformation(PKQLTransformation trans);
	
	PKQLTransformation getInvokingPkqlTransformation();
	
	void setPkqlStr(String pkqlStr);
	
	String getPkqlStr();
	
}
