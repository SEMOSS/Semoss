package prerna.ui.components.playsheets.datamakers;

import java.util.List;
import java.util.Map;

import prerna.engine.api.IScriptReactor;

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
}
