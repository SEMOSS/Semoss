package prerna.ui.components.playsheets.datamakers;

import java.util.Map;

public interface ISEMOSSAction {
	
	/**
	 * Special key to distinguish that something is an action
	 */
	String TYPE = "Type";
	
	/**
	 * Setter for the id of the action
	 * @param id
	 */
	void setId(String id);
	
	/**
	 * Getter for the id of the action
	 * @return
	 */
	String getId();
	
	/**
	 * Getter for the properties describing the parameters for the action
	 * @return
	 */
	Map<String, Object> getProperties();
	
	/**
	 * Setter for the properties describing the parameters for the action
	 * @param props
	 */
	void setProperties(Map<String, Object> props);
	
	/**
	 * Setter for the data makers for the action
	 * @param dms
	 */
	void setDataMakers(IDataMaker... dms);
	
	/**
	 * Key method for processing the action
	 * @return
	 */
	Object runMethod();

	/**
	 * Setter for the data maker component of the action
	 * @param dmc
	 */
	void setDataMakerComponent(DataMakerComponent dmc);	
}
