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
import java.util.Iterator;
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
public class CheckBoxSelectorListener extends AbstractListener {
	String type;
	IEngine engine;
	SelectScrollList scrollList;
	JCheckBox allElemCheckBox;
	JCheckBox recdSysCheckBox, intDHMSMSysCheckBox, notIntDHMSMSysCheckBox,theaterSysCheckBox, garrisonSysCheckBox,lowProbCheckBox, highProbCheckBox;
	Vector<String> recdSysList, intDHMSMSysList,notIntDHMSMSysList, theaterSysList, garrisonSysList, lowProbSysList, highProbSysList;

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
		recdSysCheckBox.setSelected(false);
		intDHMSMSysCheckBox.setSelected(false);
		notIntDHMSMSysCheckBox.setSelected(false);
		theaterSysCheckBox.setSelected(false);
		garrisonSysCheckBox.setSelected(false);
		lowProbCheckBox.setSelected(false);
		highProbCheckBox.setSelected(false);
	}
	
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
	
	protected Vector<String> createAndUnionIfBothFilled(Vector<String> list1,Vector<String> list2) {
		if(list1.isEmpty())
			return list2;
		if(list2.isEmpty())
			return list1;
		return createAndUnion(list1,list2);
	}
	
	protected Vector<String> createAndUnion(Vector<String> list1,Vector<String> list2) {
		Vector<String> retList = new Vector<String>();
		Iterator<String> it1 = list1.iterator();
		while(it1.hasNext()) {
			String check = it1.next();
			if(list2.contains(check)&&!retList.contains(check))
				retList.add(check);
		}
		return retList;
	}
	
	protected Vector<String> createOrUnion(Vector<String> list1,Vector<String> list2) {
		Vector<String> retList = new Vector<String>();
		Iterator<String> it1 = list1.iterator();
		while(it1.hasNext()) {
			String check = it1.next();
			if(!retList.contains(check))
				retList.add(check);
		}
		Iterator<String> it2 = list2.iterator();
		while(it2.hasNext()) {
			String check = it2.next();
			if(!retList.contains(check))
				retList.add(check);
		}
		return retList;
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
	public void setCheckBox(JCheckBox allSysCheckBox,JCheckBox recdSysCheckBox,JCheckBox intDHMSMSysCheckBox,JCheckBox notIntDHMSMSysCheckBox,JCheckBox theaterSysCheckBox,JCheckBox garrisonSysCheckBox,JCheckBox lowProbCheckBox,JCheckBox highProbCheckBox)
	{
		this.allElemCheckBox = allSysCheckBox;
		this.recdSysCheckBox = recdSysCheckBox;
		this.intDHMSMSysCheckBox = intDHMSMSysCheckBox;
		this.notIntDHMSMSysCheckBox = notIntDHMSMSysCheckBox;
		this.theaterSysCheckBox = theaterSysCheckBox;
		this.garrisonSysCheckBox = garrisonSysCheckBox;
		this.lowProbCheckBox = lowProbCheckBox;
		this.highProbCheckBox = highProbCheckBox;

		getQueryResults();
	}
	protected void getQueryResults()
	{
		recdSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'}}");
		intDHMSMSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'}}");
		notIntDHMSMSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N'}}");
		theaterSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}FILTER( !regex(str(?GT),'Garrison'))}");
		garrisonSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}FILTER( !regex(str(?GT),'Theater'))}");
		lowProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}}BINDINGS ?Prob {('Low')('Medium')('Medium-High')}");
		highProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}}BINDINGS ?Prob {('High')('Question')}");
	}
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
