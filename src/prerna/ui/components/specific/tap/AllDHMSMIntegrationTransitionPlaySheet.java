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

import prerna.error.EngineException;
import prerna.error.FileReaderException;
import prerna.ui.components.playsheets.BasicProcessingPlaySheet;
import prerna.util.Utility;

@SuppressWarnings("serial")
public class AllDHMSMIntegrationTransitionPlaySheet extends BasicProcessingPlaySheet{

	@Override
	public void createView() {
		Utility.showMessage("Success! Created all LPI reports.");
	}
	
	@Override
	public void createData() {
		AllDHMSMIntegrationTransitionCostProcessor writer;
		try {
			writer = new AllDHMSMIntegrationTransitionCostProcessor();
			writer.runAllReports();
		} catch (EngineException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		} catch (FileReaderException e) {
			e.printStackTrace();
			Utility.showError(e.getMessage());
		}
	}

}
