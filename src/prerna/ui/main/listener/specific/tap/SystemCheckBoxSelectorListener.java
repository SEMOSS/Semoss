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

/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class SystemCheckBoxSelectorListener extends CheckBoxSelectorListener {
	JCheckBox mhsSpecificCheckBox, ehrCoreCheckBox;
	Vector<String> mhsSpecificSysList, ehrCoreSysList;

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
			} else {
				unselectAllBoxes();
				scrollList.list.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		
		Vector<String> systemsToSelect = createSelectedList();
		scrollList.setSelectedValues(systemsToSelect);
	}
	
	protected void unselectAllBoxes() {
		super.unselectAllBoxes();
		mhsSpecificCheckBox.setSelected(false);
		ehrCoreCheckBox.setSelected(false);
	}
	
	protected Vector<String> createSelectedList() {
		Vector<String> systemsToSelect = super.createSelectedList();
		if(mhsSpecificCheckBox.isSelected())
			systemsToSelect=createOrUnion(mhsSpecificSysList,systemsToSelect);
		if(ehrCoreCheckBox.isSelected())
			systemsToSelect=createOrUnion(ehrCoreSysList,systemsToSelect);
		return systemsToSelect;
	}

	public void setCheckBox(JCheckBox allSysCheckBox,JCheckBox recdSysCheckBox,JCheckBox intDHMSMSysCheckBox,JCheckBox notIntDHMSMSysCheckBox,JCheckBox theaterSysCheckBox,JCheckBox garrisonSysCheckBox,JCheckBox lowProbCheckBox,JCheckBox highProbCheckBox,JCheckBox mhsSpecificCheckBox,JCheckBox ehrCoreCheckBox)
	{
		this.allElemCheckBox = allSysCheckBox;
		this.recdSysCheckBox = recdSysCheckBox;
		this.intDHMSMSysCheckBox = intDHMSMSysCheckBox;
		this.notIntDHMSMSysCheckBox = notIntDHMSMSysCheckBox;
		this.theaterSysCheckBox = theaterSysCheckBox;
		this.garrisonSysCheckBox = garrisonSysCheckBox;
		this.lowProbCheckBox = lowProbCheckBox;
		this.highProbCheckBox = highProbCheckBox;
		this.mhsSpecificCheckBox = mhsSpecificCheckBox;
		this.ehrCoreCheckBox = ehrCoreCheckBox;

		getQueryResults();
	}
	
	@Override
	protected void getQueryResults()
	{
		super.getQueryResults();
		mhsSpecificSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> 'Y'}}");
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
