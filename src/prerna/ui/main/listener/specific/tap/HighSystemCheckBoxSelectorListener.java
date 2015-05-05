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

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Vector;

import javax.swing.JCheckBox;

import prerna.engine.api.IEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.swing.custom.SelectScrollList;

public class HighSystemCheckBoxSelectorListener implements ActionListener {

	protected IEngine engine;
	protected JCheckBox allElemCheckBox;
	protected SelectScrollList scrollList;
	protected String type;

	JCheckBox ehrCoreCheckBox;
	Vector<String> ehrCoreSysList;
	
	public HighSystemCheckBoxSelectorListener(IEngine engine, SelectScrollList scrollList,JCheckBox allElemCheckBox,JCheckBox ehrCoreCheckBox) {
		this.engine = engine;
		this.scrollList = scrollList;
		this.allElemCheckBox = allElemCheckBox;
		this.type = "ActiveSystem";
		this.ehrCoreCheckBox = ehrCoreCheckBox;
		createCheckboxList();
	}

	@Override
	public void actionPerformed(ActionEvent e) {
		if(((JCheckBox)e.getSource()).getName().equals(allElemCheckBox.getName()))
		{
			if(allElemCheckBox.isSelected()) {
				unselectAllCheckBoxes();
				selectAllElements();
			} else {
				unselectAllCheckBoxes();
				scrollList.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		
		Vector<String> systemsToSelect = createSelectedList();
		scrollList.setSelectedValues(systemsToSelect);
	}
	
	/**
	 * Selects all the elements in the scroll list except total
	 */
	public void selectAllElements() {
		scrollList.selectAll();
		Vector<String> totalVect = new Vector<String>();
		totalVect.add("Total");
		scrollList.deSelectValues(totalVect);
		
	}

	protected void unselectAllCheckBoxes() {
		ehrCoreCheckBox.setSelected(false);
	}
	
	protected Vector<String> createSelectedList() {
		if(ehrCoreCheckBox.isSelected())
			return ehrCoreSysList;
		else
			return new Vector<String>();
	}

	protected void createCheckboxList()
	{
		ehrCoreSysList = getList(engine, type, "SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/EHR_Core> 'Y'}}");
	}
	/**
	 * Gets the list of all capabilities for a selected functional area
	 * @param sparqlQuery 		String containing the query to get all capabilities for a selected functional area
	 * @return capabilities		Vector<String> containing list of all capabilities for a selected functional area
	 */
	public Vector<String> getList(IEngine engine, String type, String sparqlQuery)
	{
		Vector<String> retList=new Vector<String>();
		try{
			EntityFiller filler = new EntityFiller();
			filler.engineName = engine.getEngineName();
			filler.type = type;
			filler.setExternalQuery(sparqlQuery);
			filler.run();
			Vector names = filler.nameVector;
			for (int i = 0;i<names.size();i++) {
				retList.add((String) names.get(i));
			}
		}catch(RuntimeException e) {
			System.out.println("Error creating checkboses for engine "+engine+" and type "+type);
		}
		return retList;
		
	}
}
