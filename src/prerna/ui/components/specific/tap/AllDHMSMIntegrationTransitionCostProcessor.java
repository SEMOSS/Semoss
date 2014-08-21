package prerna.ui.components.specific.tap;

import java.util.HashSet;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.rdf.engine.api.IEngine;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;

public class AllDHMSMIntegrationTransitionCostProcessor {

	public void runAllReports() throws EngineException, FileReaderException{
		IEngine hrCore = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(hrCore==null) {
				throw new EngineException("Database not found");
		}
		
		HashSet<String> lpiSystemList = DHMSMTransitionUtility.runRawVarListQuery(hrCore, DHMSMTransitionUtility.LPI_SYS_QUERY);
		DHMSMIntegrationTransitionCostWriter writer = new DHMSMIntegrationTransitionCostWriter();
		for(String sysURI: lpiSystemList) {
			writer.setSysURI(sysURI);
			writer.calculateValuesForReport();
			writer.writeToExcel();
		}
	}
}
