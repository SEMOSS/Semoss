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

import java.util.Vector;

import javax.swing.JCheckBox;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.swing.custom.SelectScrollList;

public class SystemCheckBoxSelectorListener extends CheckBoxSelectorListener {
	
	protected JCheckBox recdSysCheckBox, intDHMSMSysCheckBox, notIntDHMSMSysCheckBox,theaterSysCheckBox, garrisonSysCheckBox,lowProbCheckBox, highProbCheckBox;
	protected Vector<String> recdSysList, intDHMSMSysList,notIntDHMSMSysList, theaterSysList, garrisonSysList, lowProbSysList, highProbSysList;

	public SystemCheckBoxSelectorListener(IEngine engine,SelectScrollList scrollList,JCheckBox allElemCheckBox,JCheckBox recdSysCheckBox,JCheckBox intDHMSMSysCheckBox,JCheckBox notIntDHMSMSysCheckBox,JCheckBox theaterSysCheckBox,JCheckBox  garrisonSysCheckBox,JCheckBox lowProbCheckBox,JCheckBox highProbCheckBox) {
		super(engine,scrollList,"ActiveSystem",allElemCheckBox);
		this.recdSysCheckBox = recdSysCheckBox;
		this.intDHMSMSysCheckBox = intDHMSMSysCheckBox;
		this.notIntDHMSMSysCheckBox = notIntDHMSMSysCheckBox;
		this.theaterSysCheckBox = theaterSysCheckBox;
		this.garrisonSysCheckBox = garrisonSysCheckBox;
		this.lowProbCheckBox = lowProbCheckBox;
		this.highProbCheckBox = highProbCheckBox;
		createCheckboxList();
	}
	
	@Override
	protected void unselectAllCheckBoxes() {
		recdSysCheckBox.setSelected(false);
		intDHMSMSysCheckBox.setSelected(false);
		notIntDHMSMSysCheckBox.setSelected(false);
		theaterSysCheckBox.setSelected(false);
		garrisonSysCheckBox.setSelected(false);
		lowProbCheckBox.setSelected(false);
		highProbCheckBox.setSelected(false);
	}
	
	@Override
	protected Vector<String> createSelectedList() {
		Vector<String> interfaced = new Vector<String>();
		if(intDHMSMSysCheckBox.isSelected())
			interfaced = intDHMSMSysList;
		if(notIntDHMSMSysCheckBox.isSelected())
			interfaced = createOrUnion(interfaced,notIntDHMSMSysList);
	
		Vector<String> theatGarr = new Vector<String>();
		if(theaterSysCheckBox.isSelected())
			theatGarr = theaterSysList;
		if(garrisonSysCheckBox.isSelected())
			theatGarr = createOrUnion(theatGarr,garrisonSysList);
		
		Vector<String> lowHigh = new Vector<String>();
		if(lowProbCheckBox.isSelected())
			lowHigh = lowProbSysList;
		if(highProbCheckBox.isSelected())
			lowHigh = createOrUnion(lowHigh,highProbSysList);
		
		Vector<String> systemsToSelect = new Vector<String>();
		if(recdSysCheckBox.isSelected())
			systemsToSelect=createAndUnionIfBothFilled(recdSysList,systemsToSelect);
		systemsToSelect=createAndUnionIfBothFilled(interfaced,systemsToSelect);
		systemsToSelect=createAndUnionIfBothFilled(theatGarr,systemsToSelect);
		systemsToSelect=createAndUnionIfBothFilled(lowHigh,systemsToSelect);
		return systemsToSelect;
	}
	
	/**
	 * Creates the list of elements to select for each checkbox
	 */
	@Override
	protected void createCheckboxList()
	{
		recdSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'}}");
		intDHMSMSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'}}");
		notIntDHMSMSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N'}}");
		theaterSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}FILTER( !regex(str(?GT),'Garrison'))}");
		garrisonSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}FILTER( !regex(str(?GT),'Theater'))}");
		lowProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}}BINDINGS ?Prob {('Low')('Medium')('Medium-High')}");
		highProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}}BINDINGS ?Prob {('High')('Question')}");
	}
}
