package prerna.ui.components.specific.tap;

import java.util.HashSet;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.poi.specific.TAPLegacySystemDispositionReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.util.DIHelper;

public class AllLegacySystemsDispositionProcessor {

	private IEngine hr_Core;
	private String lpiListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'}} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";
	private String lpniListQuery = "SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>} {?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'} {?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Probability} {?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N'}} BINDINGS ?Probability {('Low')('Medium')('Medium-High')}";


	public void processReports() throws EngineException, FileReaderException
	{
		hr_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(hr_Core == null) {
			throw new EngineException("Could not find HR_Core db");
		}

		TAPLegacySystemDispositionReportWriter indiviudalSysWriter = new TAPLegacySystemDispositionReportWriter();
		
		HashSet<String> lpiList = indiviudalSysWriter.processSingleUniqueReturnQuery(hr_Core, lpiListQuery);
		indiviudalSysWriter.setLpiList(lpiList);
		HashSet<String> lpniList = indiviudalSysWriter.processSingleUniqueReturnQuery(hr_Core, lpniListQuery);
		indiviudalSysWriter.setLpniList(lpniList);

		for(String s : lpiList) {
			indiviudalSysWriter.setSysURI(s);
			indiviudalSysWriter.writeToExcel();
		}
		
		for(String s : lpniList) {
			indiviudalSysWriter.setSysURI(s);
			indiviudalSysWriter.writeToExcel();
		}
	}
}
