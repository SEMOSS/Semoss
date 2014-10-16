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
import java.util.ArrayList;
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
	JCheckBox recdSysButton, intDHMSMSysButton, notIntDHMSMSysButton,theaterSysCheckBox, garrisonSysCheckBox,lowProbCheckBox, highProbCheckBox;
	ArrayList<String> recdSysList, intDHMSMSysList,notIntDHMSMSysList, theaterSysList, garrisonSysList, lowProbSysList, highProbSysList;

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
				recdSysButton.setSelected(false);
				intDHMSMSysButton.setSelected(false);
				notIntDHMSMSysButton.setSelected(false);
				theaterSysCheckBox.setSelected(false);
				garrisonSysCheckBox.setSelected(false);
				lowProbCheckBox.setSelected(false);
				highProbCheckBox.setSelected(false);
				scrollList.selectAll();
			}
			else
			{
				recdSysButton.setSelected(false);
				intDHMSMSysButton.setSelected(false);
				notIntDHMSMSysButton.setSelected(false);
				theaterSysCheckBox.setSelected(false);
				garrisonSysCheckBox.setSelected(false);
				lowProbCheckBox.setSelected(false);
				highProbCheckBox.setSelected(false);
				scrollList.list.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		Vector<String> systemsToSelect = new Vector<String>();
		
		systemsToSelect = updateSystemsToSelect(recdSysButton,recdSysList,systemsToSelect);
		systemsToSelect = updateSystemsToSelect(intDHMSMSysButton,intDHMSMSysList,notIntDHMSMSysButton,notIntDHMSMSysList,systemsToSelect);
		systemsToSelect = updateSystemsToSelect(theaterSysCheckBox,theaterSysList,garrisonSysCheckBox,garrisonSysList,systemsToSelect);
		systemsToSelect = updateSystemsToSelect(lowProbCheckBox,lowProbSysList,highProbCheckBox,highProbSysList,systemsToSelect);
	
		scrollList.setSelectedValues(systemsToSelect);
	}
	public Vector<String> updateSystemsToSelect(JCheckBox button, ArrayList<String> listToIntersect, Vector<String> listOfAllSelected)
	{
		if(button.isSelected())
		{
			if(listOfAllSelected.isEmpty())
				listOfAllSelected.addAll(listToIntersect);
			else
				listOfAllSelected = addIntersection(new Vector<String>(listToIntersect), listOfAllSelected);
		}
		return listOfAllSelected;
	}
	public Vector<String> updateSystemsToSelect(JCheckBox button1, ArrayList<String> listToIntersect1,JCheckBox button2, ArrayList<String> listToIntersect2, Vector<String> listOfAllSelected)
	{
		ArrayList<String> listToIntersectCombined = new ArrayList<String>();
		
		if(button1.isSelected())
			listToIntersectCombined.addAll(listToIntersect1);
		if(button2.isSelected())
			listToIntersectCombined.addAll(listToIntersect2);
		if(button1.isSelected()||button2.isSelected())
		{
			if(listOfAllSelected.isEmpty())
				listOfAllSelected.addAll(listToIntersectCombined);
			else
				listOfAllSelected = addIntersection(new Vector<String>(listToIntersectCombined), listOfAllSelected);
		}
		return listOfAllSelected;
	}
	public Vector<String> addIntersection(Vector<String> systemsTG, Vector<String> systemsProb)
	{
		Vector<String> retList = new Vector<String>();
		Iterator<String> sysIt = systemsTG.iterator();
		while(sysIt.hasNext())
		{
			String check = sysIt.next();
			if(systemsProb.contains(check))
				retList.add(check);
		}
		return retList;
	}
		
	/**
	 * Gets the list of all capabilities for a selected functional area
	 * @param sparqlQuery 		String containing the query to get all capabilities for a selected functional area
	 * @return capabilities		Vector<String> containing list of all capabilities for a selected functional area
	 */
	public ArrayList<String> getList(String sparqlQuery)
	{

		ArrayList<String> retList=new ArrayList<String>();
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
	public void setCheckBox(JCheckBox allSysCheckBox,JCheckBox recdSysButton,JCheckBox intDHMSMSysButton,JCheckBox notIntDHMSMSysButton,JCheckBox theaterSysCheckBox,JCheckBox garrisonSysCheckBox,JCheckBox lowProbCheckBox,JCheckBox highProbCheckBox)
	{
		this.allElemCheckBox = allSysCheckBox;
		this.recdSysButton = recdSysButton;
		this.intDHMSMSysButton = intDHMSMSysButton;
		this.notIntDHMSMSysButton = notIntDHMSMSysButton;
		this.theaterSysCheckBox = theaterSysCheckBox;
		this.garrisonSysCheckBox = garrisonSysCheckBox;
		this.lowProbCheckBox = lowProbCheckBox;
		this.highProbCheckBox = highProbCheckBox;

		getQueryResults();
	}
	public void getQueryResults()
	{
		recdSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Received_Information> 'Y'}}");
		intDHMSMSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'Y'}}");
		notIntDHMSMSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Interface_Needed_w_DHMSM> 'N'}}");
		theaterSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}}BINDINGS ?GT {('Theater')('Both')}");
		garrisonSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}FILTER( !regex(str(?GT),'Theater'))}");
		lowProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Device_InterfaceYN> 'N'}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> ?Prob}}BINDINGS ?Prob {('Low')('Medium')('Medium-High')('Question')}");
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
