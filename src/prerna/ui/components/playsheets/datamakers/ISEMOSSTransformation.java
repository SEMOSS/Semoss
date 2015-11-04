package prerna.ui.components.playsheets.datamakers;

import java.util.Map;

public interface ISEMOSSTransformation {

	String TYPE = "Type";
	
	void setId(String id);
	
	String getId();
	
	Map<String, Object> getProperties();
	
	void setProperties(Map<String, Object> props);
	
	void setDataMakers(IDataMaker... dms);
	
	void runMethod();

	void setDataMakerComponent(DataMakerComponent dmc);
	
	void setTransformationType(Boolean preTransformation);
	
	void undoTransformation();
}
