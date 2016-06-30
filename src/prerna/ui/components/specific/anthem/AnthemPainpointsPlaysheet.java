package prerna.ui.components.specific.anthem;

import java.util.Hashtable;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.H2.H2Frame;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.ui.components.playsheets.datamakers.IDataMaker;

public class AnthemPainpointsPlaysheet extends TablePlaySheet implements IDataMaker {

	private static final Logger logger = LogManager.getLogger(TablePlaySheet.class.getName());
	private DataMakerComponent dmComponent;
	static String masterQuery = "SELECT DISTINCT OBA_L1.OBA_L0_FK AS OBA_L0, OBA_L1.OBA_L1 AS OBA_L1, OBA_L2.OBA_L2  OBA_L2, PAIN_POINT.PAIN_POINT_DESCRIPTION AS PAIN_POINT "
			+ "FROM OBA_L1, PAIN_POINT "
			+ "INNER JOIN OBA_L2 ON OBA_L1.OBA_L1 = OBA_L2.OBA_L1_FK AND PAIN_POINT.PAIN_POINT = OBA_L2.PAIN_POINT_FK";

	//OBA_L0.OBA_L0.OBA_L1.OBA_L0_FK

	public static String instanceOfPlaysheet = "prerna.ui.components.specific.anthem.AnthemPainpointsPlaysheet";

	//create a datamaker
	@Override
	@Deprecated
	public void createData() {
		if (this.dmComponent == null) {
			this.dmComponent = new DataMakerComponent(this.engine, masterQuery);
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

	// just calls default getDataMakerOutput
	public Map getData(Hashtable<String, Object> obj) {
		Map<String, Object> returnHashMap =  getDataMakerOutput();
		returnHashMap.put("Styling", "Anthem");
		return returnHashMap;
	}

	@Override
	public Map getDataMakerOutput(String... selectors) {
		return super.getDataMakerOutput(selectors);
	}     

}
