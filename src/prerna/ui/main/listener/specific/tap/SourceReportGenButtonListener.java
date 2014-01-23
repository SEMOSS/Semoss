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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import javax.swing.JCheckBox;
import javax.swing.JComponent;
import javax.swing.JList;
import javax.swing.JTextField;

import org.apache.log4j.Logger;

import prerna.poi.specific.ReportSheetWriter;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.api.IChakraListener;
import prerna.ui.components.specific.tap.SourceSelectPanel;
import prerna.util.Constants;
import prerna.util.ConstantsTAP;
import prerna.util.DIHelper;
import prerna.util.Utility;


/**
 * Listener for sourceReportGenButton
 */
public class SourceReportGenButtonListener implements IChakraListener {

	Logger logger = Logger.getLogger(getClass());
	ArrayList<String> queryArray = new ArrayList<String>();
	ArrayList<String> outputArray = new ArrayList<String>();
	
	/**
	 * Method actionPerformed.
	 * @param actionevent ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent actionevent) {

		queryArray.clear();
		outputArray.clear();
				
		Hashtable<String, ArrayList<String[]>> hash = new Hashtable<String, ArrayList<String[]>>();
		ReportSheetWriter writer = new ReportSheetWriter();
		JTextField RFPNameField=(JTextField)DIHelper.getInstance().getLocalProp(ConstantsTAP.RFP_NAME_FIELD);
		writer.RFPName = RFPNameField.getText();
		String workingDir = System.getProperty("user.dir");
		String folder = "\\export\\Reports\\";
		String writeFileName = "Report Source Selector " + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx";
		String fileLoc = workingDir + folder + writeFileName;
		String templateLoc = workingDir + folder + "Report_Template.xlsx";
		logger.info(fileLoc);
		
		//get queries from question sheet
		int queryCount = 1;
		String query=DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_QUERY + "_"+queryCount);
		if(query==null)
		{
			Utility.showMessage("The database does not contain the required elements");
			return;
		}
		while(query!=null)
		{
			queryArray.add(query);
			outputArray.add(DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_NAME +"_" + queryCount));
			queryCount++;
			query=DIHelper.getInstance().getProperty(ConstantsTAP.SOURCE_SELECT_REPORT_QUERY+"_" + queryCount);
		}
		
		//get list of all capabilites to pull from
		SourceSelectPanel sourceSelectPanel = (SourceSelectPanel) DIHelper.getInstance().getLocalProp(Constants.SOURCE_SELECT_PANEL);
		Enumeration<String> enumKey = sourceSelectPanel.checkBoxHash.keys();
		String capabilityList = "";
			while(enumKey.hasMoreElements()) {
			    String key = enumKey.nextElement();
				JCheckBox checkBox = (JCheckBox) sourceSelectPanel.checkBoxHash.get(key);
				if (checkBox.isSelected())
				{
					capabilityList+="(<http://health.mil/ontologies/Concept/Capability/"+checkBox.getText()+">)";
				}
		}

			
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");

		//for each individual sheet add to hash with output as name of sheet.
		
		for(int ind=0;ind<queryArray.size();ind++)
		{
			ArrayList<String[]> list = new ArrayList<String[]>();
			String[] names=null;
			
			Hashtable<String, String> capHash = new Hashtable<String, String>();
			
			capHash.put("FillCapability", capabilityList);				
			String nFillQuery = Utility.fillParam(queryArray.get(ind), capHash);
			SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(nFillQuery);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
			
			names = wrapper.getVariables();
			try {
				while(wrapper.hasNext()) {
					SesameJenaSelectStatement sjss = wrapper.next();
					String [] values = new String[names.length];
					for(int colIndex = 0;colIndex < names.length;colIndex++) {
						if(sjss.getVar(names[colIndex]) != null) {
							values[colIndex] = (String)sjss.getVar(names[colIndex]);
						}
					}
					list.add(values);
				}
			} 
			catch (Exception e) {
				e.printStackTrace();
			}
			list.add(0,names);
			hash.put(outputArray.get(ind), list);
		}
		hash.put(outputArray.get(9), processICDs(hash.get(outputArray.get(9))));
		writer.ExportLoadingSheets(fileLoc, hash,templateLoc,outputArray);
		
		
		query="SELECT DISTINCT ?Data_Object ?Description ?DataElement WHERE {{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Task_Needs_Data_Object <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?Task ?Task_Needs_Data_Object ?Data_Object}{?Data_Object <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?belongs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/BelongsTo>;} {?DataElement <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataElement> ;} {?DataElement ?belongs ?Data_Object.}OPTIONAL{?Data_Object <http://semoss.org/ontologies/Relation/Contains/Description> ?Description}} ORDER BY ?Data_Object ?DataElement BINDINGS ?Capability {@FillCapability@}";
		ArrayList<String[]> list = new ArrayList<String[]>();
		String[] names=null;
		
		Hashtable<String, String> capHash = new Hashtable<String, String>();
		
		capHash.put("FillCapability", capabilityList);				
		String nFillQuery = Utility.fillParam(query, capHash);
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(nFillQuery);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		
		names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				String [] values = new String[names.length];
				for(int colIndex = 0;colIndex < names.length;colIndex++) {
					if(sjss.getVar(names[colIndex]) != null) {
						values[colIndex] = (String)sjss.getVar(names[colIndex]);
					}
				}
				list.add(values);
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}
		list.add(0,names);

		String writeDataFileName = "Report Source Selector Data Objects " + DateFormat.getDateTimeInstance(DateFormat.MEDIUM, DateFormat.SHORT).format(new Date()).replace(":", "") + ".xlsx";
		String dataFileLoc = workingDir + folder + writeDataFileName;
		writer.exportDataWorkbook(dataFileLoc,list);
	
		logger.info("Source Report Generator Button Pushed");

	}
	
	/**
	 * Method processICDs.
	 * @param list ArrayList<String[]>
	 * @return ArrayList<String[]> 
	 */
	public ArrayList<String[]> processICDs(ArrayList<String[]> list)
	{
		
		String query="SELECT DISTINCT ?System WHERE {{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;}{?Phase <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Phase> ;}{?System ?Phase ?LifeCycle ;}} BINDINGS ?LifeCycle {(<http://health.mil/ontologies/Concept/LifeCycle/Retired_(Not_Supported)>)}";
		JList repoList = (JList)DIHelper.getInstance().getLocalProp(Constants.REPO_LIST);
		Object[] repo = (Object[])repoList.getSelectedValues();
		IEngine engine = (IEngine)DIHelper.getInstance().getLocalProp(repo[0]+"");

		ArrayList<String> decomSystems = new ArrayList<String>();
		String[] names=null;
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.executeQuery();
		
		names = wrapper.getVariables();
		try {
			while(wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				decomSystems.add((String)sjss.getVar(names[0]));
			}
		} 
		catch (Exception e) {
			e.printStackTrace();
		}

		List<String> systemsToInclude = Arrays.asList("AHLTA","AHLTA-M","AHLTA-T","CDR","CHCS","CIS-Essentris","TMDI","TMDS");
		ArrayList<String[]> ret = new ArrayList<String[]>();
		ret.add(list.get(0));
		for (String[] entry : list)
		{
			if((systemsToInclude.contains(entry[1])||systemsToInclude.contains(entry[2]))&&!decomSystems.contains(entry[1])&&!decomSystems.contains(entry[2]))
				ret.add(entry);
		}
		return ret;
	}
	
	/**
	 * Override method from IChakraListener
	 * @param view
	 */
	@Override
	public void setView(JComponent view) {
	}

}
