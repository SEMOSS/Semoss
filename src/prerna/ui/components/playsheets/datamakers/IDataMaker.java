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

	void processDataMakerComponent(DataMakerComponent component);

	void processPreTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms);

	void processPostTransformations(DataMakerComponent dmc, List<ISEMOSSTransformation> transforms, IDataMaker... dataFrame);

	Map<String, Object> getDataMakerOutput(String... selectors);

	List<Object> processActions(DataMakerComponent dmc, List<ISEMOSSAction> actions, IDataMaker... dataMaker);
	
	List<Object> getActionOutput();
}
