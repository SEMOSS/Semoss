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
package prerna.ui.main.listener.impl;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.Insight;
import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.AbstractEngine;
import prerna.ui.components.ComboboxToolTipRenderer;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.StringNumericComparator;

/**
 * Controls selection of the perspective from the left hand pane.
 */
public class PerspectiveSelectionListener extends AbstractListener {
	public JComponent view = null;
	Hashtable model = null;
	static final Logger logger = LogManager.getLogger(PerspectiveSelectionListener.class.getName());	
	
	// needs to find what is being selected from event
	// based on that refresh the view of questions for that given perspective
	
	/**
	 * Method actionPerformed.  Dictates what actions to take when an Action Event is performed.
	 * @param e ActionEvent - The event that triggers the actions in the method.
	 */
	@Override
	public void actionPerformed(ActionEvent e) {
		JComboBox bx = (JComboBox)e.getSource();
		String perspective = "";
		if(bx.getSelectedItem() != null) {
			perspective = bx.getSelectedItem().toString();
		}
		if(!perspective.isEmpty()) {
			logger.info("Selected " + perspective + " <> " + view);
			JComboBox qp = (JComboBox)view;

			ArrayList tTip = new ArrayList();
			qp.removeAllItems();
			JList list = (JList) DIHelper.getInstance().getLocalProp(
					Constants.REPO_LIST);
			// get the selected repository
			List selectedValuesList = list.getSelectedValuesList();
			String selectedVal = selectedValuesList.get(selectedValuesList.size()-1).toString();
			Vector questionsV = new Vector();

			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(selectedVal);

			try
			{
				questionsV = engine.getInsights(perspective);
				if(questionsV != null){
					//recreate qyestionsV with appended order number
					Vector questionsVCopy = new Vector(questionsV);
					
					questionsV.clear();
					for(int itemIndex = 0;itemIndex < questionsVCopy.size();itemIndex++){
						
						//if the same question is used multiple times in different perspectives, vectorInsight will contain all those insights.
						//we need to loop through the insights and find the question that belongs to the perspective selected to get the correct order #
						Vector<Insight> vectorInsight = ((AbstractEngine)engine).getInsight2((String)questionsVCopy.get(itemIndex));
						Insight in = null;
						if(vectorInsight.size() > 1){
							for(Insight insight: vectorInsight){
								if(insight.getId().contains(perspective)){
									in = insight;
								}
							}
						} else {
							in = vectorInsight.get(0);
						}
						String order = in.getOrder();
						//check to see if user has the most up-to-date XML structure to include order property;
						//if not, assume the question label itself has the order and add it straight to the vector
						if(order != null) {
							questionsV.add(order + ". " + questionsVCopy.get(itemIndex));
						} else {
							questionsV.add(questionsVCopy.get(itemIndex));
						}
					}
				}
				
			}catch(RuntimeException ex)
			{
				ex.printStackTrace();
			}
			//Hashtable questions = DIHelper.getInstance().getQuestions(perspective);
			StringNumericComparator comparator = new StringNumericComparator();
			if(questionsV != null)
			{
				Collections.sort(questionsV, comparator);
				for(int itemIndex = 0;itemIndex < questionsV.size();itemIndex++)
				{
					tTip.add(questionsV.get(itemIndex));
					
					ComboboxToolTipRenderer renderer = new ComboboxToolTipRenderer();
					qp.setRenderer(renderer);
					renderer.setTooltips(tTip);
					renderer.setBackground(Color.WHITE);
					qp.addItem(questionsV.get(itemIndex) );
				}
			}
		}

	}

	/**
	 * Method setView. Sets a JComponent that the listener will access and/or modify when an action event occurs.  
	 * @param view the component that the listener will access
	 */
	@Override
	public void setView(JComponent view) {
		logger.debug("View is set " + view);
		this.view = view;
		
	}


}
