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

import java.util.Vector;

import javax.swing.JCheckBox;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.swing.custom.SelectScrollList;
/**
 * Determines which functional areas the user wants to incorporate in RFP report
 * Used to determine if user wants to include HSD, HSS, or FHP functional areas in RFP report
 * Will populate sourceSelectPanel with all capabilities included in functional areas
 */
public class CapCheckBoxSelectorListener extends CheckBoxSelectorListener {
	JCheckBox dhmsmCapCheckBox, hsdCapCheckBox, hssCapCheckBox, fhpCapCheckBox;
	Vector<String> dhmsmCapList, hsdCapList, hssCapList, fhpCapList;

	
	public CapCheckBoxSelectorListener(IEngine engine,SelectScrollList scrollList,JCheckBox allElemCheckBox,JCheckBox dhmsmCapCheckBox,JCheckBox hsdCapCheckBox,JCheckBox hssCapCheckBox,JCheckBox fhpCapCheckBox) {
		
		super(engine,scrollList,"Capability",allElemCheckBox);
		this.dhmsmCapCheckBox = dhmsmCapCheckBox;
		this.hsdCapCheckBox = hsdCapCheckBox;
		this.hssCapCheckBox = hssCapCheckBox;
		this.fhpCapCheckBox = fhpCapCheckBox;
		createCheckboxList();
	}
			
	@Override
	protected void unselectAllCheckBoxes() {
		dhmsmCapCheckBox.setSelected(false);
		hsdCapCheckBox.setSelected(false);
		hssCapCheckBox.setSelected(false);
		fhpCapCheckBox.setSelected(false);
	}

	@Override
	protected Vector<String> createSelectedList() {
		Vector<String> capabilities = new Vector<String>();
		if(dhmsmCapCheckBox.isSelected())
			capabilities.addAll(dhmsmCapList);
		if(hsdCapCheckBox.isSelected())
			capabilities.addAll(hsdCapList);
		if(hssCapCheckBox.isSelected())
			capabilities.addAll(hssCapList);
		if(fhpCapCheckBox.isSelected())
			capabilities.addAll(fhpCapList);
		return capabilities;
	}
	
	@Override
	public void createCheckboxList()
	{
		hsdCapList = getList("Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD>)}");
		hssCapList = getList("Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS>)}");
		fhpCapList = getList("Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP>)}");
		dhmsmCapList = getList("Select DISTINCT ?entity WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?entity;}}");
	}
}
