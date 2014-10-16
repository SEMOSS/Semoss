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
import java.util.Vector;

import javax.swing.JCheckBox;
import javax.swing.JComponent;

import prerna.ui.swing.custom.SelectScrollList;
/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class CapCheckBoxSelectorListener extends CheckBoxSelectorListener {
	JCheckBox dhmsmCapCheckBox, hsdCapCheckBox, hssCapCheckBox, fhpCapCheckBox;
	Vector<String> dhmsmCapList, hsdCapList, hssCapList, fhpCapList;

	/**
	 * Determines if the user has selected HSD, HSS, FHP check box's in MHS TAP to include functional areas to include in RFP report
	 * Will populate sourceSelectPanel to show all capabilities for the functional area's selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if(((JCheckBox)e.getSource()).getName().equals(allElemCheckBox.getName()))
		{
			if(allElemCheckBox.isSelected())
			{
				dhmsmCapCheckBox.setSelected(false);
				hsdCapCheckBox.setSelected(false);
				hssCapCheckBox.setSelected(false);
				fhpCapCheckBox.setSelected(false);
				scrollList.selectAll();
			}
			else
			{
				dhmsmCapCheckBox.setSelected(false);
				hsdCapCheckBox.setSelected(false);
				hssCapCheckBox.setSelected(false);
				fhpCapCheckBox.setSelected(false);
				scrollList.list.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		Vector<String> capabilities = new Vector<String>();
		if(dhmsmCapCheckBox.isSelected())
			capabilities.addAll(dhmsmCapList);
		if(hsdCapCheckBox.isSelected())
			capabilities.addAll(hsdCapList);
		if(hssCapCheckBox.isSelected())
			capabilities.addAll(hssCapList);
		if(fhpCapCheckBox.isSelected())
			capabilities.addAll(fhpCapList);

		scrollList.setSelectedValues(capabilities);

	}
	public void setScrollList(SelectScrollList scrollList)
	{
		this.scrollList = scrollList;
		this.type = "Capability";
	}
	public void setCheckBox(JCheckBox allElemCheckBox, JCheckBox dhmsmCapCheckBox,JCheckBox hsdCapCheckBox,JCheckBox hssCapCheckBox,JCheckBox fhpCapCheckBox)
	{
		this.allElemCheckBox = allElemCheckBox;
		this.dhmsmCapCheckBox = dhmsmCapCheckBox;
		this.hsdCapCheckBox = hsdCapCheckBox;
		this.hssCapCheckBox = hssCapCheckBox;
		this.fhpCapCheckBox = fhpCapCheckBox;

		getQueryResults();
	}
	public void getQueryResults()
	{
		hsdCapList = getList("Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD>)}");
		hssCapList = getList("Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS>)}");
		fhpCapList = getList("Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP>)}");
		dhmsmCapList = getList("Select DISTINCT ?entity WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?entity;}}");
	}
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
