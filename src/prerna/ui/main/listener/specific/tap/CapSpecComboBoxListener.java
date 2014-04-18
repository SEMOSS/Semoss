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
import java.awt.event.ActionListener;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JTextField;

/**
 */
public class CapSpecComboBoxListener implements ActionListener{

	JComponent item;
	JTextField dataSelectQueryField, bluSelectQueryField;
	String engineName;
	/**
	 * Method actionPerformed.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox comboBox=(JComboBox) e.getSource();
		String selectedItem = comboBox.getSelectedItem().toString();
		if(selectedItem.equals("Select Individual Capabilities"))
		{
			item.setVisible(true);
		}
		else
		{
			item.setVisible(false);
		}

		String dataQuery = "SELECT DISTINCT ?Data WHERE {@FILL@{?CapabilityTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityTag>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?Needs <http://semoss.org/ontologies/Relation/Contains/CRM> 'C'}{?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?CapabilityTag ?TaggedBy ?Capability}{?Capability ?Consists ?Task.}{?Task ?Needs ?Data.} }";
		String bluQuery = "SELECT DISTINCT ?BLU WHERE { @FILL@{?CapabilityTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityTag>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>} {?Task_Needs_BusinessLogicUnit <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>}{?CapabilityTag ?TaggedBy ?Capability}{?Capability ?Consists ?Task.}{?Task ?Task_Needs_BusinessLogicUnit ?BLU}}";

		if(engineName.contains("TAP"))
		{
			dataQuery = "SELECT DISTINCT ?Data WHERE {@FILL@{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability> ;}{?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;}{ ?Capability ?supports ?BusinessProcess.} {?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?BusinessProcess ?consists ?Activity.}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?Data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject> ;}{?Activity ?needs ?Data.}}";
			bluQuery = "SELECT DISTINCT ?BLU WHERE {@FILL@{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?supports <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Supports> ;}{?BusinessProcess <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessProcess> ;}{ ?Capability ?supports ?BusinessProcess.} {?consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;} {?Activity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Activity> ;}{?BusinessProcess ?consists ?Activity.}{?needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;} {?BLU <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit> ;}{?Activity ?needs ?BLU.}}";
		}

		if(selectedItem.contains("HSD"))
		{
			dataQuery = dataQuery.replace("@FILL@","BIND(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD> AS ?CapabilityFunctionalArea){?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;}");
			bluQuery = bluQuery.replace("@FILL@","BIND(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD> AS ?CapabilityFunctionalArea){?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;}");
		}
		else if(selectedItem.contains("HSS"))
		{
			dataQuery = dataQuery.replace("@FILL@","BIND(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS> AS ?CapabilityFunctionalArea){?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;}");
			bluQuery = bluQuery.replace("@FILL@","BIND(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS> AS ?CapabilityFunctionalArea){?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;}");
		}
		else if(selectedItem.contains("FHP"))
		{
			dataQuery = dataQuery.replace("@FILL@","BIND(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP> AS ?CapabilityFunctionalArea){?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;}");
			bluQuery = bluQuery.replace("@FILL@","BIND(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP> AS ?CapabilityFunctionalArea){?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;}");
		}
		else if(selectedItem.contains("DHMSM"))
		{
			dataQuery = dataQuery.replace("@FILL@","{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?DHMSM ?TaggedBy ?Capability;}");
			bluQuery = bluQuery.replace("@FILL@","{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?DHMSM ?TaggedBy ?Capability;}");
		}
		else
		{
			dataQuery = dataQuery.replace("@FILL@","");
			bluQuery = bluQuery.replace("@FILL@","");
		}
		dataSelectQueryField.setText(dataQuery);
		bluSelectQueryField.setText(bluQuery);

	}
	
	/**
	 * Method setShowItem.
	 * @param component JComponent
	 */
	public void setShowItem (JComponent component)
	{
		this.item = component;
	}
	public void setEngineName(String engineName)
	{
		this.engineName = engineName;
	}
	
	public void setQueryTextFields(JTextField dataSelectQueryField,JTextField bluSelectQueryField)
	{
		this.dataSelectQueryField = dataSelectQueryField;
		this.bluSelectQueryField = bluSelectQueryField;
	}
}
