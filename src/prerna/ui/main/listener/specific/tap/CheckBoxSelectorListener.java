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
import java.awt.event.ActionListener;
import java.util.Iterator;
import java.util.Vector;

import javax.swing.JCheckBox;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.ui.helpers.EntityFiller;
import prerna.ui.swing.custom.SelectScrollList;


/**
 * Listener that selects elements in the scrollList based upon checkbox selections.
 * Used primarily in the System Optimization and DHMSM Deployment modules.
 */
public class CheckBoxSelectorListener implements ActionListener {
	private static final Logger LOGGER = LogManager.getLogger(CheckBoxSelectorListener.class.getName());

	protected IEngine engine;
	protected JCheckBox allElemCheckBox;
	protected SelectScrollList scrollList;
	protected String type;

	public CheckBoxSelectorListener(IEngine engine,SelectScrollList scrollList,String type,JCheckBox allElemCheckBox) {
		this.engine = engine;
		this.scrollList = scrollList;
		this.allElemCheckBox = allElemCheckBox;
		this.type = type;
	}
	
	/**
	 * Run when the user selects or deselects one of the checkboxes.
	 * Determines what checkboxes are selected and then selects the corresponding elements in the scroll list.
	 * If the all element checkbox is selected, all the other checkboxes will be cleared and all elements selected.
	 * @param e ActionEvent
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		if(((JCheckBox)e.getSource()).getName().equals(allElemCheckBox.getName()))
		{
			if(allElemCheckBox.isSelected()) {
				unselectAllCheckBoxes();
				selectAllElements();
			} else {
				unselectAllCheckBoxes();
				scrollList.list.clearSelection();
			}
			return;
		}
		allElemCheckBox.setSelected(false);
		
		Vector<String> systemsToSelect = createSelectedList();
		scrollList.setSelectedValues(systemsToSelect);
	}
	
	protected void selectAllElements() {
		scrollList.selectAll();
	}
	
	/**
	 * unselects all the checkboxes
	 */
	protected void unselectAllCheckBoxes() {
	}
	
	/**
	 * Creates the list of elements to select based on checkboxes selected
	 * @return
	 */
	protected Vector<String> createSelectedList() {
		return null;
	}
	
	/**
	 * Helper method to determining the list of systems.
	 * Performs an AND union on two lists if neither is empty.
	 * If one is empty, returns the other list.
	 * @param list1
	 * @param list2
	 * @return
	 */
	protected Vector<String> createAndUnionIfBothFilled(Vector<String> list1,Vector<String> list2) {
		if(list1.isEmpty())
			return list2;
		if(list2.isEmpty())
			return list1;
		return createAndUnion(list1,list2);
	}
	
	/**
	 * Helper method to determining the list of systems.
	 * Performs an AND union on two lists.
	 * @param list1
	 * @param list2
	 * @return
	 */
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
	
	/**
	 * Helper method to determining the list of systems.
	 * Performs an OR union on two lists.
	 * @param list1
	 * @param list2
	 * @return
	 */
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
			for (int i = 0;i<names.size();i++) {
				retList.add((String) names.get(i));
			}
		}catch(RuntimeException e) {
			LOGGER.error("Error creating checkboses for engine "+engine+" and type "+type);
		}
		return retList;
		
	}
	
	/**
	 * Creates the list of items to select for each checkbox
	 */
	protected void createCheckboxList() {
	}
}
