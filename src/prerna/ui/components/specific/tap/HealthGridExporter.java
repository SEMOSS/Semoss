/*******************************************************************************
 * Copyright 2013 SEMOSS.ORG
 * 
 * This file is part of SEMOSS.
 * 
 * SEMOSS is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * SEMOSS is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with SEMOSS.  If not, see <http://www.gnu.org/licenses/>.
 ******************************************************************************/
package prerna.ui.components.specific.tap;

import java.awt.event.ActionListener;
import java.beans.PropertyVetoException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.JButton;
import javax.swing.JDesktopPane;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.CONUSMapPlaySheet;
import prerna.ui.helpers.PlaysheetCreateRunner;
import prerna.ui.main.listener.impl.ChartImageExportListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * This allows the application health grid to be exported.
 */
public class HealthGridExporter {

	Logger logger = Logger.getLogger(getClass());
	
	/**
	 * Constructor for HealthGridExporter.
	 */
	public HealthGridExporter()
	{
	}
	
	/**
	 * Goes through system categories and health grid categories and creates new health grid if it is a new category.
	 * Sets up the layout and query to create the grid.
	 * Resends data to the health grid to highlight the appropriate system.
	 * Sets the location for exporting the application health grid for future use.
	 * @param systemList 	List of systems.
	 * @param catHash 		HashMap<String,String> of different categories.
	 */
	public void processData(ArrayList<String> systemList)
	{	
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data");
		String id = "Health_Grid";
		String question = QuestionPlaySheetStore.getInstance().getIDCount() + ". "+id;
		String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) ?SustainmentBudget ?SystemStatus ?highlight WHERE {BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?highlight){?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} {?System <http://semoss.org/ontologies/Relation/Phase> ?SystemStatus }BIND(1 AS ?SustainmentBudget) } BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";
		
		HealthGridSheet playSheet = new HealthGridSheet();					
		playSheet.setQuery(query);
		playSheet.setRDFEngine((IEngine) engine);
		playSheet.setQuestionID(question);
		JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		playSheet.setJDesktopPane(pane);

		QuestionPlaySheetStore.getInstance().put(question, playSheet);
		
		playSheet.setSystemHighlight(true);
		playSheet.setSystemToHighlight("ABACUS");
		playSheet.createData();
		playSheet.runAnalytics();
		playSheet.createView();
				
		for(String system: systemList)
		{
			query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) ?SustainmentBudget ?SystemStatus ?highlight WHERE {BIND(<http://health.mil/ontologies/Concept/System/ABACUS> AS ?highlight){?SystemOwner <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemOwner>;} {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} {?System <http://semoss.org/ontologies/Relation/Phase> ?SystemStatus }BIND(1 AS ?SustainmentBudget) } BINDINGS ?SystemOwner {(<http://health.mil/ontologies/Concept/SystemOwner/Army>)(<http://health.mil/ontologies/Concept/SystemOwner/Air_Force>)(<http://health.mil/ontologies/Concept/SystemOwner/Navy>)}";
			query = query.replace("ABACUS",system);
			playSheet.setQuery(query);
			playSheet.setSystemHighlight(true);
			playSheet.setSystemToHighlight("ABACUS");	
			playSheet.createData();
			playSheet.runAnalytics();
			playSheet.createView();



			if(!playSheet.isEmpty())
			{
				//location of export
				String workingDir = System.getProperty("user.dir");
				String folder = "\\export\\Images\\";
				String writeFileName = system.replaceAll(":","")+"_Health_Grid_Export.png";
				String fileLoc = workingDir + folder+writeFileName;
				
				//call chartimageexportlistener to export the app health grid.
				ChartControlPanel chartControl= playSheet.getControlPanel();
				JButton btnImageExport = chartControl.getImageExportButton();
				ActionListener[] actionList = btnImageExport.getActionListeners();
				ChartImageExportListener chartList = (ChartImageExportListener)actionList[0];
				chartList.setAutoExport(true);
				chartList.setFileLoc(fileLoc);
				chartList.setScaleBool(true);
				chartList.setScale(750,425);
				chartList.setCropBool(true);
				chartList.setCrop(27,100,435,295);
				btnImageExport.doClick();
				
//				try {
//					playSheet.setClosed(true);
//				} catch (PropertyVetoException e) {
//					e.printStackTrace();
//				}

			}
			playSheet.clearTables();

		}
	}
}
