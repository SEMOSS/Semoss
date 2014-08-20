package prerna.ui.components.specific.tap;

import java.util.HashMap;

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.poi.specific.TAPLegacySystemDispositionReportWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.util.DHMSMTransitionUtility;
import prerna.util.DIHelper;

public class AllLegacySystemsDispositionProcessor {

	private IEngine hr_Core;

	public void processReports() throws EngineException, FileReaderException
	{
		hr_Core = (IEngine) DIHelper.getInstance().getLocalProp("HR_Core");
		if(hr_Core == null) {
			throw new EngineException("Could not find HR_Core db");
		}

		TAPLegacySystemDispositionReportWriter indiviudalSysWriter = new TAPLegacySystemDispositionReportWriter();
		HashMap<String,String> reportTypeHash = DHMSMTransitionUtility.processReportTypeQuery(hr_Core);
		indiviudalSysWriter.setReportTypeHash(reportTypeHash);

		for(String s : reportTypeHash.keySet()) {
			indiviudalSysWriter.setSysURI(s);
			indiviudalSysWriter.writeToExcel();
		}
	}
}
