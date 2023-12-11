/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import java.awt.event.ActionEvent;
import java.io.IOException;

import javax.swing.JComponent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.openrdf.repository.RepositoryException;
import org.openrdf.rio.RDFHandlerException;

import prerna.engine.api.IDatabaseEngine;
import prerna.ui.components.specific.tap.CreateFutureStateDHMSMDatabase;
import prerna.ui.components.specific.tap.GLItemGeneratorSelfReportedFutureInterfaces;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.util.Utility;

public class CreateFutureInterfaceDatabaseListener extends AbstractListener{

	static final Logger LOGGER = LogManager.getLogger(CreateFutureInterfaceDatabaseListener.class.getName());

//	public static void main(String[] args) {
////		TestUtilityMethods.loadDIHelper();
////		
////		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\FutureDB.smss";
////		BigDataEngine coreEngine = new BigDataEngine();
////		coreEngine.setEngineName("FutureDB");
////		coreEngine.open(engineProp);
////		//TODO: put in correct db name
////		coreEngine.setEngineName("FutureDB");
////		DIHelper.getInstance().getCoreProp().setProperty("FutureDB_OWL", "C:\\workspace\\Semoss_Dev\\db\\FutureDB\\FutureDB_OWL.OWL");
////		DIHelper.getInstance().setLocalProperty("FutureDB", coreEngine);
////		
////		engineProp = "C:\\workspace\\Semoss_Dev\\db\\FutureCostDB.smss";
////		coreEngine = new BigDataEngine();
////		coreEngine.setEngineName("FutureCostDB");
////		coreEngine.open(engineProp);
////		//TODO: put in correct db name
////		coreEngine.setEngineName("FutureCostDB");
////		DIHelper.getInstance().getCoreProp().setProperty("FutureCostDB_OWL", "C:\\workspace\\Semoss_Dev\\db\\FutureCostDB\\FutureCostDB_OWL.OWL");
////		DIHelper.getInstance().setLocalProperty("FutureCostDB", coreEngine);
////		
////		engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data.smss";
////		coreEngine = new BigDataEngine();
////		coreEngine.setEngineName("TAP_Core_Data");
////		coreEngine.open(engineProp);
////		//TODO: put in correct db name
////		coreEngine.setEngineName("TAP_Core_Data");
////		DIHelper.getInstance().setLocalProperty("TAP_Core_Data", coreEngine);
////		
////		engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Cost_Data.smss";
////		coreEngine = new BigDataEngine();
////		coreEngine.setEngineName("TAP_Cost_Data");
////		coreEngine.open(engineProp);
////		//TODO: put in correct db name
////		coreEngine.setEngineName("TAP_Cost_Data");
////		DIHelper.getInstance().setLocalProperty("TAP_Cost_Data", coreEngine);
////		
////		CreateFutureInterfaceDatabaseListener l = new CreateFutureInterfaceDatabaseListener();
////		l.actionPerformed(null);
//	}
	
	@Override
	public void actionPerformed(ActionEvent arg0) {
		//get selected values
//		JComboBox<String> tapCoreDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_Core_Data_FUTURE_INTERFACE_DATABASE_CORE_COMBO_BOX);
//		String tapCoreName = tapCoreDBComboBox.getSelectedItem() + "";
		String tapCoreName = "TAP_Core_Data"; //tapCoreDBComboBox.getSelectedItem() + "";
		
		
//		JComboBox<String> futureStateDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_FUTURE_INTERFACE_DATABASE_COMBO_BOX);
//		String futureDBName = futureStateDBComboBox.getSelectedItem() + "";
		String futureDBName = "FutureDB";
		
//		JComboBox<String> futureStateCostDBComboBox = (JComboBox<String>) DIHelper.getInstance().getLocalProp(ConstantsTAP.TAP_FUTURE_COST_INTERFACE_DATABASE_COMBO_BOX);
//		String futureCostDBName = futureStateCostDBComboBox.getSelectedItem() + "";
		String futureCostDBName = "FutureCostDB";

		//get associated engines
		IDatabaseEngine tapCoreDB = Utility.getDatabase(tapCoreName);
		IDatabaseEngine futureDB = Utility.getDatabase(futureDBName);
		IDatabaseEngine futureCostDB = Utility.getDatabase(futureCostDBName);
		
		//send to processor
		LOGGER.info("Creating " + futureDBName + " from " + tapCoreName);
		LOGGER.info("Creating " + futureCostDBName + " from " + tapCoreName);
		
		try {
			IDatabaseEngine tapCost = Utility.getDatabase("TAP_Cost_Data");
			if(tapCost == null) {
				throw new IOException("Cost Info Not Found");
			}
			
			GLItemGeneratorSelfReportedFutureInterfaces glGen = new GLItemGeneratorSelfReportedFutureInterfaces(tapCoreDB, futureDB, futureCostDB);
			glGen.genData();
			
			CreateFutureStateDHMSMDatabase futureStateCreator = new CreateFutureStateDHMSMDatabase(tapCoreDB, futureDB, futureCostDB);
			futureStateCreator.addTriplesToExistingICDs();
			futureStateCreator.generateData();
			futureStateCreator.createDBs();
			Utility.showMessage("Finished adding triples to " + futureDBName + " and " + futureCostDBName);
		} catch (IOException e) {
			Utility.showError("Error with generting new DB. Make sure DB's are properly defined.");
			e.printStackTrace();
		} 
		catch (RepositoryException e) {
			Utility.showError("Error with generting new DB");
			e.printStackTrace();
		} catch (RDFHandlerException e) {
			Utility.showError("Error with generting new DB");
			e.printStackTrace();
		}
	}

	@Override
	public void setView(JComponent view) {
		// TODO Auto-generated method stub

	}

}
