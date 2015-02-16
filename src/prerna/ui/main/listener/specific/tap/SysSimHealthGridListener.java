/*******************************************************************************
 * Copyright 2015 SEMOSS.ORG
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.ui.main.listener.specific.tap;

import javax.swing.JDesktopPane;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.components.specific.tap.HealthGridSheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.main.listener.impl.AbstractBrowserSPARQLFunction;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

import com.google.gson.Gson;
import com.teamdev.jxbrowser.chromium.JSValue;

/**
 */
public class SysSimHealthGridListener extends AbstractBrowserSPARQLFunction {
	
	GraphPlaySheet gps;
	String coreDb = "TAP_Core_Data";
	String coreInstanceSystemURI = "http://health.mil/ontologies/Concept/System/";

	/**
	 * Method invoke.
	 * @param arg0 Object[]
	 * @return Object 
	 */
	@Override
	public JSValue invoke(JSValue... arg0) {
		Gson gson = new Gson();
		String sysArrayString = arg0[0].getString();
		String[] sysArray = gson.fromJson(sysArrayString, String[].class);
		
		String sysOfInterest = sysArray[0];
		
		HealthGridSheet hgs = new HealthGridSheet();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(coreDb);
		String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) (COALESCE(?SustainmentBud,0.0) AS ?SustainmentBudget) (COALESCE(?status, \"TBD\") AS ?SystemStatus) ?highlight WHERE { BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?highlight){?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud} } OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv} } OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm} } OPTIONAL { {?System <http://semoss.org/ontologies/Relation/Phase> ?status }{?status <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/LifeCycle> } } } BINDINGS ?System {@SystemList@}";
		query = query.replace("ABACUS",sysOfInterest);
		String sysBindingList= "";
		
		for (int i=0; i<sysArray.length;i++)
		{
			sysBindingList=sysBindingList+"(<" + coreInstanceSystemURI+sysArray[i] + ">)";
		}
		query = query.replaceAll("@SystemList@", sysBindingList);	
		
		
		
		String question ="System Similarity HealthGrid Custom";
		hgs.setTitle("System Similarity HealthGrid for "+ sysOfInterest);
		hgs.setQuery(query);
		hgs.setRDFEngine(engine);
		hgs.setQuestionID(question);
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance()
				.getLocalProp(Constants.DESKTOP_PANE);
		hgs.setJDesktopPane(pane);
		// need to create the playsheet create runner
		Runnable playRunner = null;
		playRunner = new PlaysheetCreateRunner(hgs);
		QuestionPlaySheetStore.getInstance().put(question, hgs);
		Thread playThread = new Thread(playRunner);
		playThread.start();
		return arg0[0];
	}
	
}
