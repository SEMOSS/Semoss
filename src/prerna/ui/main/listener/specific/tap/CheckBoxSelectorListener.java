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
	JCheckBox theaterSysCheckBox, garrisonSysCheckBox,lowProbCheckBox, medProbCheckBox,highProbCheckBox;
	ArrayList<String> theaterSysList, garrisonSysList, lowProbSysList, medProbSysList, highProbSysList;

	/**
	 * Determines if the user has selected HSD, HSS, FHP check box's in MHS TAP to include functional areas to include in RFP report
	 * Will populate sourceSelectPanel to show all capabilities for the functional area's selected
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if(e.getSource().equals(allElemCheckBox))
		{
			if(allElemCheckBox.isSelected())
			{
				theaterSysCheckBox.setSelected(false);
				garrisonSysCheckBox.setSelected(false);
				lowProbCheckBox.setSelected(false);
				medProbCheckBox.setSelected(false);
				highProbCheckBox.setSelected(false);
				scrollList.selectAll();
			}
			else
			{
				theaterSysCheckBox.setSelected(false);
				garrisonSysCheckBox.setSelected(false);
				lowProbCheckBox.setSelected(false);
				medProbCheckBox.setSelected(false);
				highProbCheckBox.setSelected(false);
				scrollList.list.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		Vector<String> systemsTG = new Vector<String>();
		Vector<String> systemsProb = new Vector<String>();
		if(theaterSysCheckBox.isSelected())
			systemsTG.addAll(theaterSysList);
		if(garrisonSysCheckBox.isSelected())
			systemsTG.addAll(garrisonSysList);
		if(lowProbCheckBox.isSelected())
			systemsProb.addAll(lowProbSysList);
		if(medProbCheckBox.isSelected())
			systemsProb.addAll(medProbSysList);
		if(highProbCheckBox.isSelected())
			systemsProb.addAll(highProbSysList);
		Vector<String> systems;
		if(systemsTG.isEmpty())
			systems = systemsProb;
		else if(systemsProb.isEmpty())
			systems = systemsTG;
		else
			systems = addIntersection(systemsTG,systemsProb);
		
		scrollList.setSelectedValues(systems);

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
		EntityFiller filler = new EntityFiller();
		filler.engineName = engine.getEngineName();
		filler.type = type;
		filler.setExternalQuery(sparqlQuery);
		filler.run();
		Vector names = filler.nameVector;
		ArrayList<String> retList=new ArrayList<String>();
		for (int i = 0;i<names.size();i++)
		{
			retList.add((String) names.get(i));
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
	public void setCheckBox(JCheckBox allSysCheckBox,JCheckBox theaterSysCheckBox,JCheckBox garrisonSysCheckBox,JCheckBox lowProbCheckBox,JCheckBox medProbCheckBox,JCheckBox highProbCheckBox)
	{
		this.allElemCheckBox = allSysCheckBox;
		this.theaterSysCheckBox = theaterSysCheckBox;
		this.garrisonSysCheckBox = garrisonSysCheckBox;
		this.lowProbCheckBox = lowProbCheckBox;
		this.medProbCheckBox = medProbCheckBox;
		this.highProbCheckBox = highProbCheckBox;

		getQueryResults();
	}
	public void getQueryResults()
	{
		theaterSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> 'Theater'}}");
		garrisonSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> 'Garrison'}}");
		lowProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'Low'}}");
		medProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'Medium'}}");
		highProbSysList = getList("SELECT DISTINCT ?entity WHERE {{?entity <http://www.w3.org/1999/02/22-rdf-syntax-ns#type>  <http://semoss.org/ontologies/Concept/ActiveSystem> ;}{?entity <http://semoss.org/ontologies/Relation/Contains/Probability_of_Included_BoS_Enterprise_EHRS> 'High'}}");
	}
	/**
	 * Override method from AbstractListener
	 * @param view JComponent
	 */
	@Override
	public void setView(JComponent view) {

	}
}
