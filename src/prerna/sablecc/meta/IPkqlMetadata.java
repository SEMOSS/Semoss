package prerna.sablecc.meta;

import java.util.Map;

import prerna.ui.components.playsheets.datamakers.PKQLTransformation;

public interface IPkqlMetadata {

	/**
	 * Returns the relevant information of the pkql
	 * @return
	 */
	Map<String, Object> getMetadata();
	
	/**
	 * Get the explanation of the PKQL executed, to be sent to the FE as a label
	 * in the recipe
	 * @return
	 */
	String getExplanation();
	
	/**
	 * Set the pkql transformation that invoked this pkql expression
	 * Used when we need to update the pkql recipe
	 * Change must be made in pkql transformation since that is used for saving
	 * @param trans
	 */
	void setInvokingPkqlTransformation(PKQLTransformation trans);
	
	/**
	 * Get the pkql transformation that invoked this pkql expression
	 * Used when we need to update the pkql recipe
	 * Change must be made in pkql transformation since that is used for saving
	 * @param trans
	 */
	PKQLTransformation getInvokingPkqlTransformation();
	
	/**
	 * Set the pkql string associated with this metadata
	 * @param pkqlStr
	 */
	void setPkqlStr(String pkqlStr);
	
	/**
	 * Get the pkql string associated with this metadata
	 * @return
	 */
	String getPkqlStr();
	
}
