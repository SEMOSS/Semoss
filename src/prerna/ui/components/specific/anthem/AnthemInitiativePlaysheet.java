package prerna.ui.components.specific.anthem;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.H2.H2Frame;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class AnthemInitiativePlaysheet extends TablePlaySheet implements IDataMaker {
	private static final Logger logger = LogManager.getLogger(TablePlaySheet.class.getName());
	private DataMakerComponent dmComponent;
//	static String masterQuery = "SELECT DISTINCT OBA_L1.OBA_L0_FK AS OBA_L0, OBA_L1.OBA_L1 AS OBA_L1, OBA_L2.OBA_L2 AS OBA_L2, INITIATIVE.INITIATIVE_NAME AS PAIN_POINT_DESCRIPTION "
//			+ "FROM OBA_L1, INITIATIVE "
//			+ "INNER JOIN OBA_L2 ON OBA_L1.OBA_L1 = OBA_L2.OBA_L1_FK AND INITIATIVE.INITIATIVE = OBA_L2.INITIATIVE_FK "
//			+ "where OBA_L2 is not null";

	private final String OBA_L0 = "OBA_L0_FK";
	private final String OBA_L1 = "OBA_L1";
	private final String OBA_L2 = "OBA_L2";
//	private final String INITIATIVE = "INITIATIVE_NAME";

	public static String instanceOfPlaysheet = "prerna.ui.components.specific.anthem.AnthemInitiativePlaysheet";

	//create a datamaker
	@Override
	public void createData() {
		if (this.dmComponent == null) {
			this.dmComponent = new DataMakerComponent(this.engine, this.query);
		}

		if(this.dataFrame == null) {
			this.dataFrame = new H2Frame();
		}
		this.dataFrame.processDataMakerComponent(this.dmComponent);
	}


	@Override
	public void setUserId(String userId) {
		if(this.dataFrame == null) {
			this.dataFrame = new H2Frame();
		}
		this.dataFrame.setUserId(userId);
	}

//	// just calls default getDataMakerOutput
//	public Map getData(Hashtable<String, Object> obj) {
//		Map<String, Object> returnHashMap =  getDataMakerOutput();
//		returnHashMap.put("Styling", "Anthem");
//		return returnHashMap;
//	}

	@Override
	public Map getDataMakerOutput(String... selectors) {
		Map<String, Object> returnHashMap = super.getDataMakerOutput(selectors);
//		returnHashMap.putAll(dataHash);
//		
//		String[] headers = (String[]) returnHashMap.get("headers");
//		headers[3] = "PAIN_POINT_DESCRIPTION";//SHOULD CHANGE THIS TO MORE GENERIC, Better if we can make the query label this field as the right name (Filter) so we don't hard hard code it here
//		returnHashMap.put("headers",headers);
//		
		returnHashMap.put("styling", "Anthem");
		returnHashMap.put("dataTableAlign", getDataTableAlign());
		return returnHashMap;
	}    
	
	public Map getDataTableAlign (){
		Map<String, String> dataTableAlign = new HashMap <String, String> ();
		dataTableAlign.put("levelOne", OBA_L0);
		dataTableAlign.put("levelTwo", OBA_L1);
		dataTableAlign.put("levelThree", OBA_L2);
		
		return dataTableAlign;
	}

}
