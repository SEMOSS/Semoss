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
package prerna.ui.components.specific.tap;

import java.beans.PropertyVetoException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JDesktopPane;
import javax.swing.JInternalFrame;
import javax.swing.JScrollPane;
import javax.swing.JTable;

import prerna.ui.components.GridFilterData;
import prerna.ui.components.GridTableModel;
import prerna.ui.components.NewScrollBarUI;
import prerna.ui.components.ParamComboBox;
import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * Calculates TAP system-specific values and outputs them to GridFilterData.
 */
public class TAPSystemSpecificCalculator {
	GridFilterData gfd = new GridFilterData();
	Date startDate = null;
	Date lastDate = null;
	Hashtable yearIdxTable = new Hashtable();
	Double hourlyRate = 200.0;
	Double sdlcTotal = 0.0;
	Double dataFedTotal = 0.0;
	Double semanticRate = 0.4;
	Boolean semantics;
	Boolean serviceBoolean;
	ArrayList runTypes;
	
	/**
	 * Constructor for TAPSystemSpecificCalculator.
	 */
	public TAPSystemSpecificCalculator()
	{
		
	}
	
	/**
	 * Sets list of run types.
	 * 
	 * @param types ArrayList	List of types to be set.
	 */
	public void setTypes(ArrayList types){
		runTypes = types;
	}
	
	/**
	 * Set boolean value of semantics used.
	 * 
	 * @param sem boolean	Value to be set for semantics
	 */
	public void setSemantics(boolean sem){
		semantics = sem;
	}
	
	/**
	 * Set boolean value of serviceBoolean.
	 * 
	 * @param ser boolean	Value to be set for semantics
	 */
	public void setServiceBoolean(boolean ser){
		serviceBoolean = ser;
	}
	
	/**
	 * Process data for all systems and create results grid
	 * 
	 * @param systemV String[]	List of systems
	 */
	public void processData(String[] systemV)
	{
		Vector sdlcVector = new Vector();
		sdlcVector.addElement("Requirements");
		sdlcVector.addElement("Design");
		sdlcVector.addElement("Develop");
		sdlcVector.addElement("Test");
		sdlcVector.addElement("Deploy");
		ArrayList <Object []> outputList = new ArrayList<Object []>();
		for (int sysIdx = 0; sysIdx < systemV.length ; sysIdx++)
		{
			EstimationCalculationFunctions pfCalc= new EstimationCalculationFunctions();
			
			 try {
					pfCalc.out= new PrintWriter(new BufferedWriter(new FileWriter("output.txt", true)));
				} catch (IOException e) {
					e.printStackTrace();
				}
			sdlcTotal = 0.0;
			dataFedTotal = 0.0;
			String system = systemV[sysIdx];
			
			int nullCount = 0;//used to see if no information exists on this system.  Then I can show error message.
			
			//String sustainLOE = pfCalc.getSysSustainData(system);
			//if(sustainLOE==null) nullCount++;
			//else if (sustainLOE != null)
			//{
			pfCalc.setHourlyRate(this.hourlyRate);
			pfCalc.setTypes(runTypes);
			pfCalc.setServiceBoolean(serviceBoolean);
			ArrayList <Object[]> phaseReturnList = pfCalc.processPhaseData(system);
			int latestIdx = 0;
//			int phaseIdx = 0;
//			int highestLOESetIdx = 1;
			int startDateIdx = 2;
//			int endDateIdx = 3;
			int totalLOEIdx = 4;
			int dataFedIdx = 5;
			for (int i = 0; i<phaseReturnList.size(); i++)
			{
				Object[] phaseReturnArray = phaseReturnList.get(i);
				Object[] phaseItems = new Object[8];
				phaseItems[0]=system;
				phaseItems[1]=sdlcVector.get(i);
				
				Double phaseLoeTotal = (Double) phaseReturnArray[totalLOEIdx];
				Date phaseStartDate = (Date) phaseReturnArray[startDateIdx];
				Double dataFedPhaseTotal = (Double) phaseReturnArray[dataFedIdx];
				//if loeTotal is not null, add loe for phase and fiscal year for phase
				if(phaseLoeTotal!=null){
					sdlcTotal = sdlcTotal+phaseLoeTotal;
					
					int yearIdx  = pfCalc.retYearIdx(phaseStartDate);
					phaseItems[yearIdx] =  phaseLoeTotal*hourlyRate;
					outputList.add(i, phaseItems);
					latestIdx = yearIdx;
				}
				//if loeTotal is null, add loe total as 0 and end date as start date.
				else{
					int yearIdx  = 2;
					if(phaseStartDate != null) {
						yearIdx = pfCalc.retYearIdx(phaseStartDate);
					}
					phaseLoeTotal = 0.0;
					phaseItems[yearIdx] =  phaseLoeTotal;
					outputList.add(i, phaseItems);
					latestIdx = yearIdx;
					nullCount++;//increase the null count.  if null for all phases, show error message
				}
				if(dataFedPhaseTotal!=null)
				{
					dataFedTotal = dataFedTotal+dataFedPhaseTotal;
				}
			}
			
			Object[] semLine = new Object[8];
			semLine[0] = system;
			semLine[1] = "Semantics";
				//get date of develop
			Object[] phaseReturnDevelopArray = phaseReturnList.get(2);
			Date semDate = (Date) phaseReturnDevelopArray[startDateIdx];
			if (semDate != null)
			{
				int semIdx = pfCalc.retYearIdx(semDate);
				Double semLOE = dataFedTotal*semanticRate*hourlyRate;
				semLine[semIdx]= semLOE;
			}

			Object[] trString = pfCalc.getSysTrainingData(system);
			Object[] trLine = new Object[8];
			trLine[0] = system;
			trLine[1] = "Training";
			
//			if(trString==null) nullCount++;
			if (trString != null)
			{
				//get date of develop
				Object[] phaseReturnArray = phaseReturnList.get(4);
				Date trDate = (Date) phaseReturnArray[startDateIdx];
				if (trDate!=null)
				{
					int trIdx = pfCalc.retYearIdx(trDate);
					Double trainingDouble = sdlcTotal*0.15;
					Double trLOE = trainingDouble*hourlyRate;
					trLine[trIdx]= trLOE;
				}
			}
			
			Object[] sustainLine = new Object[8];
			sustainLine[0] = system;
			sustainLine[1] = "Sustainment";
			Double sustainLOEDouble = sdlcTotal*0.18;
			for (int i =latestIdx+1;i<8;i++)
			{
				//Double sustainLOEDouble = sdlcTotal*0.18;
				Double sustainLOEDoubleYear = sustainLOEDouble* Math.pow(1.018, i-(latestIdx+1));
				sustainLine[i]=sustainLOEDoubleYear *hourlyRate;
			}
	
			//if all 5 phases are null and training and semantics and sustainment are null, display null error.
			/*if(nullCount == 7) {
				showError("The selected RDF store does not contain financial information for "+system +
						". \nPlease select a different system or RDF store and try again.");
			}*/
			outputList.add(sustainLine);
			outputList.add(semLine);
			outputList.add(trLine);
			pfCalc.out.close();
		}
		
		createGrid(outputList);
	}
	
	/**
	 * Creates grid of result data.
	 * 
	 * @param outputList ArrayList<Object[]>	List of data to be set
	 */
	public void createGrid(ArrayList<Object[]> outputList)
	{
		String [] names = new String[]{"System", "Phase", "FY2013","FY2014", "FY2015", "FY2016","FY2017", "FY2018"};

		JComboBox reportType = (JComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_TYPE_COMBO_BOX);
		String type = (String) reportType.getSelectedItem();
		String title = type;
		if(!type.contains("Generic")){
			ParamComboBox systemComboBox = (ParamComboBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_REPORT_COMBO_BOX);
			String system = (String) systemComboBox.getSelectedItem();
			title = system +"-"+title;
		}
		else title = "Generic-"+title;
		JCheckBox dataFedCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_FED);
		JCheckBox consumerCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_CONSUMER);
		JCheckBox bluProviderCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_BLU_PROVIDER);
		JCheckBox genericDataCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_DATA_GENERIC);
		JCheckBox genericBLUCheck = (JCheckBox) DIHelper.getInstance().getLocalProp(Constants.TRANSITION_CHECK_BOX_BLU_GENERIC);
		
		if(dataFedCheck.isSelected()&&dataFedCheck.isVisible()) title = title+"-Data Federation";
		if(bluProviderCheck.isSelected()&&bluProviderCheck.isVisible()) title = title+"-BLU Provider";
		if(consumerCheck.isSelected()&&consumerCheck.isVisible()) title = title+"-Consumer";
		if(genericDataCheck.isSelected()&&genericDataCheck.isVisible()) title = title+"-Data";
		if(genericBLUCheck.isSelected()&&genericBLUCheck.isVisible()) title = title+"-BLU";

		gfd.setColumnNames(names);
		gfd.setDataList(outputList);
		JTable table = new JTable();
		
		GridTableModel model = new GridTableModel(gfd);
		table.setModel(model);
		table.setAutoCreateRowSorter(true);
		JDesktopPane pane = (JDesktopPane)DIHelper.getInstance().getLocalProp(Constants.DESKTOP_PANE);
		JScrollPane scrollPane = new JScrollPane(table);
		scrollPane.getVerticalScrollBar().setUI(new NewScrollBarUI());
		scrollPane.setAutoscrolls(true);
		JInternalFrame financialTestSheet = new JInternalFrame();
		financialTestSheet.setContentPane(scrollPane);
		pane.add(financialTestSheet);
		financialTestSheet.setClosable(true);
		financialTestSheet.setMaximizable(true);
		financialTestSheet.setIconifiable(true);
		financialTestSheet.setTitle(title);
		financialTestSheet.setResizable(true);
		financialTestSheet.pack();
		financialTestSheet.setVisible(true);
		try {
			financialTestSheet.setSelected(false);
			financialTestSheet.setSelected(true);
		} catch (PropertyVetoException e) {
			e.printStackTrace();
		}
	}
}
