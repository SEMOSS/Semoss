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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JComponent;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.main.listener.impl.AbstractListener;
import prerna.ui.swing.custom.SelectScrollList;

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class HighSystemCheckBoxSelectorListener extends AbstractListener {
	String type;
	IEngine engine;
	SelectScrollList scrollList;
	JCheckBox allElemCheckBox, ehrCoreCheckBox;
	Vector<String> ehrCoreSysList;

	/**
	 * Determines if the user has selected HSD, HSS, FHP check box's in MHS TAP to include functional areas to include in RFP report
	 * Will populate sourceSelectPanel to show all capabilities for the functional area's selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if(((JCheckBox)e.getSource()).getName().equals(allElemCheckBox.getName()))
		{
			if(allElemCheckBox.isSelected()) {
				unselectAllBoxes();
				scrollList.selectAll();
				Vector<String> totalVect = new Vector<String>();
				totalVect.add("Total");
				scrollList.deSelectValues(totalVect);
				
			} else {
				unselectAllBoxes();
				scrollList.list.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		
		scrollList.setSelectedValues(ehrCoreSysList);
	}
	
	protected void unselectAllBoxes() {
		ehrCoreCheckBox.setSelected(false);
	}
		
	/**
	 * Gets the list of all capabilities for a selected functional area
	 * @param sparqlQuery 		String containing the query to get all capabilities for a selected functional area
	 * @return capabilities		Vector<String> containing list of all capabilities for a selected functional area
	 */
	protected Vector<String> getList(String sparqlQuery)
	{
		Vector<String> retList=new Vector<String>();
		try{
			EntityFiller filler = new EntityFiller();
			filler.engineName = engine.getEngineName();
			filler.type = type;
			filler.setExternalQuery(sparqlQuery);
			filler.run();
			Vector names = filler.nameVector;
			for (int i = 0;i<names.size();i++)
			{
				retList.add((String) names.get(i));
			}
		}catch(RuntimeException e)
		{
			System.out.println("ignored");
		}
		return retList;
		
	}

	public void setEngine(IEngine engine)
	{
		this.engine = engine;
		
	}
	public void setScrollList(SelectScrollList scrollList)
	{
		this.scrollList = scrollList;
		this.type = "ActiveSystem";
	}
	public void setCheckBox(JCheckBox allSysCheckBox,JCheckBox ehrCoreCheckBox)
	{
		this.allElemCheckBox = allSysCheckBox;
		this.ehrCoreCheckBox = ehrCoreCheckBox;

		getQueryResults();
	}
	protected void getQueryResults()
	{
		ehrCoreSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/EHR_Core> 'Y'}}");
	}
	
	
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
