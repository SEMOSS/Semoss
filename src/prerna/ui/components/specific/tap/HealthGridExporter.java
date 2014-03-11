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
import java.util.ArrayList;
import java.util.HashMap;

import javax.swing.JButton;
import javax.swing.JDesktopPane;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.api.IPlaySheet;
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
	public void processData(ArrayList<String> systemList,HashMap<String,String> catHash)
	{
		String healthGridCat="";
		Runnable playRunner = null;
		IPlaySheet playSheet = null;
		
		for(String system: systemList)
		{
			String sysCat = catHash.get(system);
			if(!sysCat.equals(healthGridCat))
			{
				//if we have a new category, create a new application health grid for that system category.
				//if we already have this category loaded, then just resend the data with new system to highlight
				healthGridCat = sysCat;
				IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Core_Data");
				
				//set up id, layout, and query
				String id = "Health_Grid";
				String question = QuestionPlaySheetStore.getInstance().getIDCount() +". "+ id;
				String layoutValue = "prerna.ui.components.specific.tap.HealthGridSheet";
				String query = "SELECT ?System (COALESCE(?bv * 100, 0.0) AS ?BusinessValue) (COALESCE(?estm, 0.0) AS ?ExternalStability) (COALESCE(?tstm, 0.0) AS ?TechnicalStandards) (COALESCE(?SustainmentBud,0.0) AS ?SustainmentBudget) ?SystemStatus WHERE {BIND(<http://health.mil/ontologies/Concept/SystemOwner/Army> AS ?SystemOwner) {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?OwnedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/OwnedBy>;}{?System ?OwnedBy ?SystemOwner}OPTIONAL{{?System <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?SustainmentBud}}OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/BusinessValue> ?bv}} OPTIONAL{ {?System <http://semoss.org/ontologies/Relation/Contains/ExternalStabilityTM> ?estm} } OPTIONAL {{?System <http://semoss.org/ontologies/Relation/Contains/TechnicalStandardTM> ?tstm}} {?System <http://semoss.org/ontologies/Relation/Phase> ?SystemStatus } }";
				query=query.replace("Army",sysCat);

				try {
					playSheet = (IPlaySheet) Class.forName(layoutValue).getConstructor(null).newInstance(null);
				} catch (Exception ex) {
					ex.printStackTrace();
					logger.fatal(ex);
				}
							
				playSheet.setQuery(query);
				playSheet.setRDFEngine((IEngine) engine);
				playSheet.setQuestionID(question);

				
				JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
				playSheet.setJDesktopPane(pane);

				// need to create the playsheet create runner
				playRunner = new PlaysheetCreateRunner(playSheet);

				// put it into the store
				QuestionPlaySheetStore.getInstance().put(question, playSheet);
				
				Thread playThread = new Thread(playRunner);
				playThread.start();
			
				try {
					Thread.sleep(4000);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				
			}
			//resending the data to the health grid so that it will highlight the current system
			((HealthGridSheet)playSheet).setSystemHighlight(true);
			((HealthGridSheet)playSheet).setSystemToHighlight(system);
			((HealthGridSheet)playSheet).refreshView();
			
			try {
				Thread.sleep(3000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			

			if(!((HealthGridSheet)playSheet).isEmpty())
			{
				//location of export
				String workingDir = System.getProperty("user.dir");
				String folder = "\\export\\Images\\";
				String writeFileName = system.replaceAll(":","")+"_Health_Grid_Export.png";
				String fileLoc = workingDir + folder+writeFileName;
				
				//call chartimageexportlistener to export the app health grid.
				ChartControlPanel chartControl= ((HealthGridSheet)playSheet).getControlPanel();
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

			}
		}
	}
}
