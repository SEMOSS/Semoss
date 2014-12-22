/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
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
