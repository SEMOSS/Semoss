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

public class HighSystemCheckBoxSelectorListener extends CheckBoxSelectorListener {

	JCheckBox ehrCoreCheckBox;
	Vector<String> ehrCoreSysList;

	
	public HighSystemCheckBoxSelectorListener(IEngine engine, SelectScrollList scrollList,JCheckBox allElemCheckBox,JCheckBox ehrCoreCheckBox) {
		super(engine, scrollList,"ActiveSystem",allElemCheckBox);
		this.ehrCoreCheckBox = ehrCoreCheckBox;
		createCheckboxList();
	}

	/**
	 * Selects all the elements in the scroll list except total
	 */
	@Override
	public void selectAllElements() {
		scrollList.selectAll();
		Vector<String> totalVect = new Vector<String>();
		totalVect.add("Total");
		scrollList.deSelectValues(totalVect);
		
	}

	@Override
	protected void unselectAllCheckBoxes() {
		ehrCoreCheckBox.setSelected(false);
	}
	
	@Override
	protected Vector<String> createSelectedList() {
		if(ehrCoreCheckBox.isSelected())
			return ehrCoreSysList;
		else
			return new Vector<String>();
	}
	
	@Override
	protected void createCheckboxList()
	{
		ehrCoreSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/EHR_Core> 'Y'}}");
	}

}
