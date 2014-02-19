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

import javax.swing.JButton;
import javax.swing.JDesktopPane;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.ChartControlPanel;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.CONUSMapPlaySheet;
import prerna.ui.main.listener.impl.ChartImageExportListener;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.QuestionPlaySheetStore;

/**
 * Creates a map of systems in the continental United States and allows the user to export the image for future use.
 */
public class CONUSMapExporter {

	Logger logger = Logger.getLogger(getClass());
	
	/**
	 * Constructor for CONUSMapExporter.
	 */
	public CONUSMapExporter()
	{
		
	}
	
	/**
	 * Runs a query to obtain systems that are contained in the TAP Site database.
	
	 * @return ArrayList<String>	List of systems in the site db. */
	public ArrayList<String> systemsInSiteDB()
	{
		ArrayList<String> systemsInSite = new ArrayList<String>();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		String query = "SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}}";		
		
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		
		String[] names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				String sys = (String)sjss.getVar(names[0]);
				systemsInSite.add(sys);
			}
		}
		catch (Exception e) {
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
	public void processData(ArrayList<String> systemList)
	{
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp("TAP_Site_Data");
		String id = "CONUS_Map";
		String question = QuestionPlaySheetStore.getInstance().getIDCount() + ". "+id;
		String layoutValue = "prerna.ui.components.playsheets.CONUSMapPlaySheet";
		String query = "SELECT DISTINCT ?System ?Facility ?lat ?lon WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?Facility <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Facility>;} {?Facility  <http://semoss.org/ontologies/Relation/Contains/LONG> ?lon}{?Facility  <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat} BIND (<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?SystemDCSite ?DeployedAt ?Facility;}{?System ?DeployedAt1 ?SystemDCSite;} }";
		IPlaySheet playSheet = null;
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

		QuestionPlaySheetStore.getInstance().put(question, playSheet);
		
		playSheet.createData();
		playSheet.runAnalytics();
		playSheet.createView();
		
		
		ArrayList<String> systemsInSite = systemsInSiteDB();
		for(String system: systemList)
		{
			if(systemsInSite.contains(system))
			{
				query = "SELECT DISTINCT ?System ?Facility ?lat ?lon WHERE { {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite> ;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?Facility <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Facility>;} {?Facility  <http://semoss.org/ontologies/Relation/Contains/LONG> ?lon}{?Facility  <http://semoss.org/ontologies/Relation/Contains/LAT> ?lat} BIND (<http://health.mil/ontologies/Concept/System/AHLTA> AS ?System){?SystemDCSite ?DeployedAt ?Facility;}{?System ?DeployedAt1 ?SystemDCSite;} }";
				query=query.replace("AHLTA",system);

				playSheet.setQuery(query);
				playSheet.setRDFEngine((IEngine) engine);
				
				((CONUSMapPlaySheet)playSheet).createView();
				((CONUSMapPlaySheet)playSheet).browser.waitReady();
				
				if(!((CONUSMapPlaySheet)playSheet).isEmpty())
				{
					//location of export
					String workingDir = System.getProperty("user.dir");
					String folder = "\\export\\Images\\";
					String writeFileName = system+"_CONUS_Map_Export.png";
					String fileLoc = workingDir + folder+writeFileName;
					
					//call chartimageexportlistener to export the conusmap. and then close the chart.
					ChartControlPanel chartControl= ((CONUSMapPlaySheet)playSheet).getControlPanel();
					JButton btnImageExport = chartControl.getImageExportButton();
					ActionListener[] actionList = btnImageExport.getActionListeners();
					ChartImageExportListener chartList = (ChartImageExportListener)actionList[0];
					chartList.setAutoExport(true);
					chartList.setFileLoc(fileLoc);
					chartList.setScaleBool(true);
					chartList.setScale(710,440);
					btnImageExport.doClick();
	
				}

			}
		}
		try {
					((CONUSMapPlaySheet)playSheet).setClosed(true);
				} catch (PropertyVetoException e) {
					e.printStackTrace();
				}
	}
}
