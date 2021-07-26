package prerna.ui.components.playsheets.datamakers;

import java.util.Map;

@Deprecated
public interface ISEMOSSTransformation {

	/**
	 * Special key to distinguish that something is an action
	 */
	String TYPE = "Type";

	/**
	 * Setter for the id of the transformation
	 * @param id
	 */
	void setId(String id);

	/**
	 * Getter for the id of the transformation
	 * @return
	 */
	String getId();

	/**
	 * Getter for the properties describing the parameters for the transformation
	 * @return
	 */
	Map<String, Object> getProperties();

	/**
	 * Setter for the properties describing the parameters for the transformation
	 * @param props
	 */
	void setProperties(Map<String, Object> props);

	/**
	 * Setter for the data makers for the transformation
	 * @param dms
	 */
	void setDataMakers(IDataMaker... dms);

	/**
	 * Key method for processing the transformation
	 * @return
	 */
	void runMethod();

	/**
	 * Setter for the data maker component of the transformation
	 * @param dmc
	 */
	void setDataMakerComponent(DataMakerComponent dmc);

	/**
	 * Setter for if the transformation is a postTransformation or preTransformation
	 * @param preTransformation
	 */
	void setTransformationType(Boolean preTransformation);

	/**
	 * Method to undo the transformation
	 */
	void undoTransformation();
	
	/**
	 * Create a copy of this transformation for storage
	 * @return
	 */
	ISEMOSSTransformation copy();
}
