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

public class SystemMHSEHRCheckBoxSelectorListener extends SystemCheckBoxSelectorListener {
	protected JCheckBox mhsSpecificCheckBox, ehrCoreCheckBox;
	protected Vector<String> mhsSpecificSysList, ehrCoreSysList;
	
	public SystemMHSEHRCheckBoxSelectorListener(IEngine engine,SelectScrollList scrollList,JCheckBox allElemCheckBox,JCheckBox recdSysCheckBox,JCheckBox intDHMSMSysCheckBox,JCheckBox notIntDHMSMSysCheckBox,JCheckBox theaterSysCheckBox,JCheckBox garrisonSysCheckBox,JCheckBox lowProbCheckBox,JCheckBox highProbCheckBox,JCheckBox mhsSpecificCheckBox,JCheckBox ehrCoreCheckBox) {
		
		super(engine,scrollList, allElemCheckBox, recdSysCheckBox, intDHMSMSysCheckBox, notIntDHMSMSysCheckBox, theaterSysCheckBox, garrisonSysCheckBox, lowProbCheckBox, highProbCheckBox);
		this.mhsSpecificCheckBox = mhsSpecificCheckBox;
		this.ehrCoreCheckBox = ehrCoreCheckBox;
		createCheckboxList();
	}
	
	@Override
	protected void unselectAllCheckBoxes() {
		super.unselectAllCheckBoxes();
		mhsSpecificCheckBox.setSelected(false);
		ehrCoreCheckBox.setSelected(false);
	}
	
	@Override
	protected Vector<String> createSelectedList() {
		Vector<String> systemsToSelect = super.createSelectedList();
		if(mhsSpecificCheckBox.isSelected())
			systemsToSelect=createOrUnion(mhsSpecificSysList,systemsToSelect);
		if(ehrCoreCheckBox.isSelected())
			systemsToSelect=createOrUnion(ehrCoreSysList,systemsToSelect);
		return systemsToSelect;
	}
	
	@Override
	protected void createCheckboxList()
	{
		super.createCheckboxList();
		mhsSpecificSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> 'Y'}}");
		ehrCoreSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/EHR_Core> 'Y'}}");
	}

}
