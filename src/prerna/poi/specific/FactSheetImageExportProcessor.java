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
package prerna.poi.specific;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.specific.tap.HealthGridExporter;
import prerna.util.DIHelper;

public class FactSheetImageExportProcessor {

	private static final Logger logger = LogManager.getLogger(FactSheetImageExportProcessor.class.getName());

	public void runImageExport() {
		//Select Systems
		ArrayList<String> sysList = new ArrayList<String>();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data");

		//for services
		String query = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?Owner}}ORDER BY ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";

		//for central
		//	String query = "SELECT DISTINCT ?System WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?Owner}}ORDER BY ?System BINDINGS ?Owner {(<http://health.mil/ontologies/Concept/SystemOwner/Central>)}";

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);
		
		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		*/
		
		String[] names = wrapper.getVariables();
		while(wrapper.hasNext()) {
			ISelectStatement sjss = wrapper.next();
			String sys = (String)sjss.getVar(names[0]);
			sysList.add(sys);
		}

		//Call the Image Exporters
		//		CONUSMapExporter conusExporter = new CONUSMapExporter();
		//		conusExporter.processData(sysList);

		//		OCONUSMapExporter oconusExporter = new OCONUSMapExporter();
		//		oconusExporter.processData(sysList);

		HealthGridExporter healthExporter = new HealthGridExporter();
		healthExporter.processData(sysList);

		logger.info("Map and Grid Export Button Pushed.");
	}

}
