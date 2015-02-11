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

import java.awt.event.ActionListener;
import java.beans.PropertyVetoException;
import java.util.ArrayList;

import javax.swing.JButton;
import javax.swing.JDesktopPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.api.ISelectStatement;
import prerna.rdf.engine.api.ISelectWrapper;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.playsheets.OCONUSMapPlaySheet;
import prerna.ui.main.listener.impl.ChartImageExportListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * Creates a map of systems in the continental United States and allows the user to export the image for future use.
 */
public class OCONUSMapExporter {

	static final Logger logger = LogManager.getLogger(OCONUSMapExporter.class.getName());

	/**
	 * Constructor for CONUSMapExporter.
	 */
	public OCONUSMapExporter()
	{

	}

	/**
	 * Runs a query to obtain systems that are contained in the TAP Site database.

	 * @return ArrayList<String>	List of systems in the site db. */
	public ArrayList<String> systemsInSiteDB()
	{
		ArrayList<String> systemsInSite = new ArrayList<String>();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		String query = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}} ORDER BY ?System";		

		ISelectWrapper wrapper = WrapperManager.getInstance().getSWrapper(engine, query);

		/*SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();*/

		String[] names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				ISelectStatement sjss = wrapper.next();
				String sys = (String)sjss.getVar(names[0]);
				systemsInSite.add(sys);
			}
		}
		catch (RuntimeException e) {
			e.printStackTrace();
		}
		return systemsInSite;
	}

	/**
	 * Uses the list of systems in order to get additional information about facility, latitude, and longitude from a different query run on the TAP site data.
	 * Creates a playsheet containing the map of the continental United States with systems plotted on it.
	 * Specifies a location for image export and closes the chart.
	 * @param systemList 	ArrayList containing all of the system names, in string form.
	 */
	public String processData(ArrayList<String> systemList)
	{
		String fileLoc = "";

		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		String id = "OCONUS_Map";
		String question = QuestionPlaySheetStore.getInstance().getIDCount() + ". "+id;

		boolean shouldStart = true;
		ArrayList<String> systemsInSite = systemsInSiteDB();
		for(String system: systemList)
		{
			//set shouldStart to true in line 97 to run all systems.
			//if crashes on export, set shouldStart in line 97 to false.
			//Then put system equals to name of system it ended on and rerun.
			//will continue with the export for this system, and any after it.
			if(system.equals("MEDBOLTS"))
				shouldStart = true;
			if(shouldStart)
			{
				if(systemsInSite.contains(system))
				{
					String query = "SELECT DISTINCT ?System ?DCSite ?lat ?lon WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?DCSite  <http://semoss.org/ontologies/Relation/Contains/LONG> ?lon}{?DCSite  <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat} BIND (<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?SystemDCSite ?DeployedAt ?DCSite;}{?System ?DeployedAt1 ?SystemDCSite;} }";
					query=query.replace("AHLTA",system);

					OCONUSMapPlaySheet playSheet = new OCONUSMapPlaySheet();					
					playSheet.setQuery(query);
					playSheet.setRDFEngine((IEngine) engine);
					playSheet.setQuestionID(question);
					JDesktopPane pane = (JDesktopPane) DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
					playSheet.setJDesktopPane(pane);

					QuestionPlaySheetStore.getInstance().put(question, playSheet);

					playSheet.createData();
					playSheet.runAnalytics();
					playSheet.createView();

					if(!playSheet.isEmpty())
					{
						//location of export
						String workingDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
						String folder = "\\export\\Images\\";
						String writeFileName = system+"_OCONUS_Map_Export.png";
						if(fileLoc.equals("")){
							fileLoc += workingDir + folder + writeFileName;
						} else {
							fileLoc += ";" + workingDir + folder + writeFileName;
						}
						//call chartimageexportlistener to export the conusmap. and then close the chart.
						ChartControlPanel chartControl= playSheet.getControlPanel();
						JButton btnImageExport = chartControl.getImageExportButton();
						ActionListener[] actionList = btnImageExport.getActionListeners();
						ChartImageExportListener chartList = (ChartImageExportListener)actionList[0];
						chartList.setAutoExport(true);
						chartList.setFileLoc(fileLoc);
						chartList.setScaleBool(true);
						chartList.setScale(710,440);
						btnImageExport.doClick();
						try {
							playSheet.setClosed(true);
						} catch (PropertyVetoException e) {
							e.printStackTrace();
						}
					}
				}
			}
		}
		return fileLoc;
	}
}
