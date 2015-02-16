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

import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.GridFilterData;

/**
 * This class is used to calculate the cost of generic services for a system.
 */
public class PfTapGenericServicesCalculator {
	
	static final Logger logger = LogManager.getLogger(PfTapGenericServicesCalculator.class.getName());
	
	GridFilterData gfd = new GridFilterData();
	Date startDate = null;
	Date lastDate = null;
	Hashtable yearIdxTable = new Hashtable();
	Hashtable systemPhaseEstimate = new Hashtable();
	Hashtable systemPhaseDate = new Hashtable();
	ArrayList runTypes = new ArrayList();
	Boolean serviceBoolean;
	
	/**
	 * Constructor for PfTapGenericServicesCalculator.
	 */
	public PfTapGenericServicesCalculator()
	{
		
	}
	/**
	 * Sets the types.
	 * @param types 	List of types.
	 */
	public void setTypes(ArrayList types){
		runTypes = types;
	}
	/**
	 * Sets service boolean.
	 * @param ser 	True if it is a service.
	 */
	public void setServiceBoolean(boolean ser){
		serviceBoolean = ser;
	}
	
	/**
	 * Processes data about a certain system.
	 * Includes information about SDLC, LOE, start date, and end date.
	 * @param system 	Name of system, in string form.
	 */
	public void processData(String system)
	{
		ArrayList <String []> outputList = new ArrayList();
		Vector phaseV = new Vector();
		phaseV.addElement("Requirements");
		phaseV.addElement("Design");
		phaseV.addElement("Develop");
		phaseV.addElement("Test");
		phaseV.addElement("Deploy");
		Double sdlcTotal = 0.0;
		
		EstimationCalculationFunctions pfCalc= new EstimationCalculationFunctions();
		pfCalc.setTypes(runTypes);
		pfCalc.setServiceBoolean(serviceBoolean);
				

		ArrayList <Object[]> phaseReturnList = pfCalc.processPhaseData(system);
//		int phaseIdx = 0;
//		int highestLOESetIdx = 1;
		int startDateIdx = 2;
//		int endDateIdx = 3;
		int totalLOEIdx = 4;
		for (int i = 0; i<phaseReturnList.size(); i++)
		{
			Object[] phaseReturnArray = phaseReturnList.get(i);

			Double phaseLoeTotal = (Double) phaseReturnArray[totalLOEIdx];
			Date phaseStartDate = (Date) phaseReturnArray[startDateIdx];
			//if loeTotal is not null, add loe for phase and fiscal year for phase
			if(phaseLoeTotal!=null){
				sdlcTotal = sdlcTotal+phaseLoeTotal;
				systemPhaseEstimate.put((String)phaseV.get(i), phaseLoeTotal);
				systemPhaseDate.put((String)phaseV.get(i), pfCalc.retFiscalYear(phaseStartDate));
			}
			//if loeTotal is null, add loe total as 0 and end date as start date.
			else{

				phaseLoeTotal = 0.0;
				systemPhaseEstimate.put((String)phaseV.get(i), 0.0);
				systemPhaseDate.put((String)phaseV.get(i), 2014);
			}
		}
		
		Object[] semString = pfCalc.getSysSemData(system);
		systemPhaseEstimate.put("Semantics", 0.0);
		systemPhaseDate.put("Semantics", 2014);
		Object[] trString = pfCalc.getSysTrainingData(system);
		systemPhaseEstimate.put("Training", new Double(0.0));
		systemPhaseDate.put("Training", 2014);
		if (trString != null)
		{
			//get date of develop
			Object[] phaseReturnArray = phaseReturnList.get(4);
			Date trDate = (Date) phaseReturnArray[startDateIdx];

//			int trIdx = pfCalc.retYearIdx(trDate);
			Double trainingDouble = sdlcTotal*0.15;
			systemPhaseEstimate.put("Training", trainingDouble);
			systemPhaseDate.put("Training", pfCalc.retFiscalYear(trDate));
		}
		
		//logger.info(systemPhaseEstimate);
		//logger.info(systemPhaseDate);
		PfTapFinancialOrganizerFinal tapOrg = new PfTapFinancialOrganizerFinal();
		tapOrg.preparePhaseTasks(systemPhaseEstimate, systemPhaseDate, system);
		tapOrg.createGrid();
		
	}
	
}
