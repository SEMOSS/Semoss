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
import java.util.List;
import java.util.Vector;

import prerna.algorithm.impl.specific.tap.SysOptUtilityMethods;
import prerna.engine.api.IEngine;
import prerna.util.ListUtilityMethods;

/**
 * This playsheet provides the selected lists based on checkboxes selections
 */
@SuppressWarnings("serial")
public class SysOptCheckboxListUpdater {
	
	IEngine engine;
	
	//lists corresponding to system checkboxes
	private Vector<String> recdSysList, intDHMSMSysList,notIntDHMSMSysList, theaterSysList, garrisonSysList, lowProbSysList, highProbSysList, mhsSpecificSysList, ehrCoreSysList;
	//lists corresponding to capability checkboxes
	private Vector<String> allCapList, dhmsmCapList, hsdCapList, hssCapList, fhpCapList;
	//lists corresponding to data checkboxes
	private Vector<String> allDataList, dhmsmDataList, hsdDataList, hssDataList, fhpDataList;
	//lists corresponding to blu checkboxes
	private Vector<String> allBLUList, dhmsmBLUList, hsdBLUList, hssBLUList, fhpBLUList;

	public SysOptCheckboxListUpdater(IEngine engine) {
		this.engine = engine;
		createSystemCheckBoxLists(false);
		createCapabilityCheckBoxLists();
		createDataAndBLUCheckBoxLists();
	}
	
	public SysOptCheckboxListUpdater(IEngine engine, Boolean runSystem, Boolean runFullCap, Boolean runDataBLU) {
		this.engine = engine;
		if(runSystem)
			createSystemCheckBoxLists(false);
		if(runFullCap)
			createCapabilityCheckBoxLists();
		else
			dhmsmCapList = SysOptUtilityMethods.getList(engine,"Capability","Select DISTINCT ?entity WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?entity;}}");
		if(runDataBLU)
			createDataAndBLUCheckBoxLists();
	}
	
	public SysOptCheckboxListUpdater(IEngine engine, Boolean runSystem, Boolean runCap, Boolean runDataBLU, Boolean costGreaterThanZero) {
		this.engine = engine;
		if(runSystem)
			createSystemCheckBoxLists(costGreaterThanZero);
		if(runCap)
			createCapabilityCheckBoxLists();
		if(runDataBLU)
			createDataAndBLUCheckBoxLists();
	}
	
	private void createSystemCheckBoxLists(Boolean costGreaterThanZero) {
		String costGreaterThanZeroTriples;
		if(costGreaterThanZero)
			costGreaterThanZeroTriples = "{?entity <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}FILTER(?cost > 0)";
		else
			costGreaterThanZeroTriples = "";
		recdSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'}"+ costGreaterThanZeroTriples + "}");
		intDHMSMSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'}"+ costGreaterThanZeroTriples + "}");
		notIntDHMSMSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N'}"+ costGreaterThanZeroTriples + "}");
		theaterSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}FILTER( !regex(str(?GT),'Garrison'))"+ costGreaterThanZeroTriples + "}");
		garrisonSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}FILTER( !regex(str(?GT),'Theater'))"+ costGreaterThanZeroTriples + "}");
		lowProbSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}"+ costGreaterThanZeroTriples + "}BINDINGS ?Prob {('Low')('Medium')('Medium-High')}");
		highProbSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}"+ costGreaterThanZeroTriples + "}BINDINGS ?Prob {('High')('Question')}");
		mhsSpecificSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/MHS_Specific> 'Y'}"+ costGreaterThanZeroTriples + "}");
		ehrCoreSysList = SysOptUtilityMethods.getList(engine,"ActiveSystem","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/EHR_Core> 'Y'}"+ costGreaterThanZeroTriples + "}");
	}
	
	private void createCapabilityCheckBoxLists() {
		//TODO should not go through task anymore? should be tagged by entity?
		allCapList = SysOptUtilityMethods.getList(engine,"Capability","SELECT DISTINCT ?entity WHERE {{?CapabilityTag <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityTag>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/Capability> ;}{?CapabilityTag ?TaggedBy ?entity}}");
		dhmsmCapList = SysOptUtilityMethods.getList(engine,"Capability","Select DISTINCT ?entity WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?entity;}}");
		hsdCapList = SysOptUtilityMethods.getList(engine,"Capability","Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD>)}");
		hssCapList = SysOptUtilityMethods.getList(engine,"Capability","Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS>)}");
		fhpCapList = SysOptUtilityMethods.getList(engine,"Capability","Select DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?entity;}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP>)}");
	}
	
	public void createDataAndBLUCheckBoxLists()
	{
		//TODO should not go through task anymore?
		allDataList = SysOptUtilityMethods.getList(engine,"DataObject","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}}");
		dhmsmDataList = SysOptUtilityMethods.getList(engine,"DataObject","SELECT DISTINCT ?entity WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.}}");
		hsdDataList = SysOptUtilityMethods.getList(engine,"DataObject","SELECT DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD>)}");
		hssDataList = SysOptUtilityMethods.getList(engine,"DataObject","SELECT DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS>)}");
		fhpDataList = SysOptUtilityMethods.getList(engine,"DataObject","SELECT DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>;}{?Task ?Needs ?entity.}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP>)}");
	
		allBLUList = SysOptUtilityMethods.getList(engine,"BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}}");
		dhmsmBLUList = SysOptUtilityMethods.getList(engine,"BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?DHMSM <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DHMSM>;}{?TaggedBy <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/TaggedBy>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?DHMSM ?TaggedBy ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?Task ?Needs ?entity.}}");
		hsdBLUList = SysOptUtilityMethods.getList(engine,"BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?Task ?Needs ?entity.}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSD>)}");
		hssBLUList = SysOptUtilityMethods.getList(engine,"BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?Task ?Needs ?entity.}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/HSS>)}");
		fhpBLUList = SysOptUtilityMethods.getList(engine,"BusinessLogicUnit","SELECT DISTINCT ?entity WHERE {{?CapabilityFunctionalArea <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityFunctionalArea>;}{?Utilizes <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Utilizes>;}{?CapabilityGroup <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/CapabilityGroup>;}{?CapabilityFunctionalArea ?Utilizes ?CapabilityGroup;}{?ConsistsOfCapability <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?CapabilityGroup ?ConsistsOfCapability ?Capability;} {?Consists <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Consists>;}{?Task <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Task>;}{?Capability ?Consists ?Task.}{?Needs <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Needs>;}{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>;}{?Task ?Needs ?entity.}} BINDINGS ?CapabilityFunctionalArea {(<http://health.mil/ontologies/Concept/CapabilityFunctionalArea/FHP>)}");
	}
	
	public Vector<String> getReceivedSysList() {
		return recdSysList;
	}
	
	public List<String> getSelectedSystemList(Vector<String> capList, Boolean intDHMSM, Boolean notIntDHMSM, Boolean theater, Boolean garrison, Boolean low, Boolean high, Boolean mhsSpecific, Boolean ehrCore) {
		List<String> checkboxSysList = getSelectedSystemList(intDHMSM, notIntDHMSM, theater, garrison, low, high, mhsSpecific, ehrCore);
		if(checkboxSysList == null || checkboxSysList.isEmpty())
			checkboxSysList = recdSysList;
		
		List<String> capabilitySysList = SysOptUtilityMethods.getList(engine,"Capability","SELECT DISTINCT ?entity WHERE { {?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?Capability <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Capability>;}{?entity <http://semoss.org/ontologies/Relation/Supports> ?Capability}} BINDINGS ?Capability {" + SysOptUtilityMethods.makeBindingString("Capability",new ArrayList<String>(capList)) + "}");	
		
		return ListUtilityMethods.createAndUnionIfBothFilled(checkboxSysList, capabilitySysList);
	}
	
	public List<String> getSelectedSystemList(Boolean intDHMSM, Boolean notIntDHMSM, Boolean theater, Boolean garrison, Boolean low, Boolean high, Boolean mhsSpecific, Boolean ehrCore) {
		List<String> interfaced = new Vector<String>();
		if(intDHMSM)
			interfaced = intDHMSMSysList;
		if(notIntDHMSM)
			interfaced = ListUtilityMethods.createOrUnion(interfaced,notIntDHMSMSysList);
	
		List<String> theatGarr = new Vector<String>();
		if(theater)
			theatGarr = theaterSysList;
		if(garrison)
			theatGarr = ListUtilityMethods.createOrUnion(theatGarr,garrisonSysList);
		
		List<String> lowHigh = new Vector<String>();
		if(low)
			lowHigh = lowProbSysList;
		if(high)
			lowHigh = ListUtilityMethods.createOrUnion(lowHigh,highProbSysList);
		
		List<String> systemsToSelect = new Vector<String>();
		systemsToSelect=ListUtilityMethods.createAndUnionIfBothFilled(interfaced,systemsToSelect);
		systemsToSelect=ListUtilityMethods.createAndUnionIfBothFilled(theatGarr,systemsToSelect);
		systemsToSelect=ListUtilityMethods.createAndUnionIfBothFilled(lowHigh,systemsToSelect);

		if(mhsSpecific)
			systemsToSelect=ListUtilityMethods.createOrUnion(mhsSpecificSysList,systemsToSelect);
		if(ehrCore)
			systemsToSelect=ListUtilityMethods.createOrUnion(ehrCoreSysList,systemsToSelect);

		return systemsToSelect;
	}
	
	public Vector<String> getAllCapabilityList() {
		return allCapList;
	}
	
	public Vector<String> getDHMSMCapabilityList() {
		return dhmsmCapList;
	}
	
	public Vector<String> getSelectedCapabilityList(Boolean dhmsm, Boolean hsd, Boolean hss, Boolean fhp) {
		Vector<String> capabilities = new Vector<String>();
		if(dhmsm)
			capabilities.addAll(dhmsmCapList);
		if(hsd)
			capabilities.addAll(hsdCapList);
		if(hss)
			capabilities.addAll(hssCapList);
		if(fhp)
			capabilities.addAll(fhpCapList);
		return capabilities;
	}
	
	public Vector<String> getAllDataList() {
		return allDataList;
	}
	
	public Vector<String> getSelectedDataList(Boolean dhmsm, Boolean hsd, Boolean hss, Boolean fhp) {
		Vector<String> dataObjects = new Vector<String>();
		if(dhmsm)
			dataObjects.addAll(dhmsmDataList);
		if(dhmsm)
			dataObjects.addAll(hsdDataList);
		if(hss)
			dataObjects.addAll(hssDataList);
		if(fhp)
			dataObjects.addAll(fhpDataList);
		return dataObjects;
	}
	
	public Vector<String> getAllBLUList() {
		return allBLUList;
	}
	
	public Vector<String> getSelectedBLUList(Boolean dhmsm, Boolean hsd, Boolean hss, Boolean fhp) {
		Vector<String> blus = new Vector<String>();
		if(dhmsm)
			blus.addAll(dhmsmBLUList);
		if(dhmsm)
			blus.addAll(hsdBLUList);
		if(hss)
			blus.addAll(hssBLUList);
		if(fhp)
			blus.addAll(fhpBLUList);
		return blus;
	}
	
	
}
