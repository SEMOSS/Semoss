package prerna.ui.components.playsheets.datamakers;

import java.util.Map;

public interface ISEMOSSAction {

	String TYPE = "Type";
	
	void setId(String id);
	
	String getId();
	
	Map<String, Object> getProperties();
	
	void setProperties(Map<String, Object> props);
	
	void setDataMakers(IDataMaker... dms);
	
	Object runMethod();

	void setDataMakerComponent(DataMakerComponent dmc);	
}
