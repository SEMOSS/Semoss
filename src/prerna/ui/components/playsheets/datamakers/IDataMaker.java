package prerna.ui.components.playsheets.datamakers;

import java.util.List;
import java.util.Map;

/**
 * This Interface defines responsibilities of a data maker
 * Data makers are used to generate the data necessary for a view
 * Data makers are fed data maker components one by one and the maker performs necessary actions to consume component
 * 
 * @author bisutton
 *
 */
public interface IDataMaker {

	enum DATA_FRAME_REACTORS {IMPORT_DATA, COL_ADD};
	
	void processDataMakerComponent(DataMakerComponent component);

	void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms);

	void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame);

	Map<String, Object> getDataMakerOutput(String... selectors);

	List<Object> processActions(DataMakerComponent dmc, List<ISEMOSSAction> actions, IDataMaker... dataMaker);
	
	List<Object> getActionOutput();

	Map<String, String> getScriptReactors();
	
	/**
	 * Used to update the data id when data has changed within the frame
	 */
	void updateDataId();
	
	/**
	 * Returns the current data id
	 * @return 
	 */
	int getDataId();
	
	/**
	 * reset the dataId to be 0
	 */
	void resetDataId();
	
	/**
	 * Sets the name of the user who created this instance of the data maker
	 * @param userId
	 */
	void setUserId(String userId);
	
	/**
	 * Returns the name of the user who created this instance of the data maker
	 * @return
	 */
	String getUserId();
	
	/**
	 * Returns the name of the data maker
	 * This name must match that which is defined within RDF_MAP
	 * @return
	 */
	String getDataMakerName();
}
