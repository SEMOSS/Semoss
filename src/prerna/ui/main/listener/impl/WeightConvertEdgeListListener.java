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
package prerna.ui.main.listener.impl;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;

import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import prerna.om.SEMOSSEdge;
import prerna.ui.components.GraphToTreeConverter;
import prerna.ui.components.WeightDropDownButton;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.EdgeStrokeTransformer;

/**
 * 
 */
public class WeightConvertEdgeListListener implements ListSelectionListener{

	GraphPlaySheet playSheet;
	GraphToTreeConverter converter;
	WeightDropDownButton btnMenu;
	double minimumValue = 1;
	double multiplier = 3;
	
	/**
	 * Constructor for WeightConvertEdgeListListener.
	 */
	public WeightConvertEdgeListListener(){
		converter = new GraphToTreeConverter();
	}
	
	/**
	 * Method setPlaySheet.  Sets the play sheet that the listener will access.
	 * @param ps
	 */
	public void setPlaySheet(GraphPlaySheet ps){
		this.playSheet = ps;
	}
	
	/**
	 * Method setButtonMenu.  Sets the button menu that the listener will access.
	 * @param bt
	 */
	public void setButtonMenu(WeightDropDownButton bt){
		this.btnMenu = bt;
	}
	
	/**
	 * Method valueChanged.  Retrieves information for a play sheet when it is selected from the list.
	 * @param e ListSelectionEvent
	 */
	@Override
	public void valueChanged(ListSelectionEvent e) {
		converter.setPlaySheet(playSheet);
		JList list = (JList) e.getSource();
		String prop = (String) list.getSelectedValue();
		Double highValue = null;
		Double lowValue = null;
		Collection<SEMOSSEdge> edgeArray = playSheet.forest.getEdges();
		Hashtable edgeWeightHash = new Hashtable();
		EdgeStrokeTransformer est = (EdgeStrokeTransformer)playSheet.getView().getRenderContext().getEdgeStrokeTransformer();
		//if prop is null deselection happened and we will just reset everything
		if(prop ==null)
		{
			est.setEdges(null);
			playSheet.getView().repaint();
			return;
		}
		for (SEMOSSEdge edge: edgeArray)
		{			
			if(edge.propHash.containsKey(prop) && edge.propHash.get(prop) instanceof Double) {
				double value = (Double)edge.propHash.get(prop);
				if(highValue == null)
					highValue = value;
				if (lowValue == null)
					lowValue = value;
				if (value>highValue)
					highValue = value;
				if (value<lowValue)
					lowValue = value;
				
				edgeWeightHash.put(edge.getURI(), value);
			} 
		}
		//repeat loop since we need highValue nad lowValue to not be null
		for (SEMOSSEdge edge: edgeArray)
		{			
			if(!edge.propHash.containsKey(prop)) {
				est.setDash(true);
			} 
		}
		
		if(highValue==null || lowValue==null ||highValue.equals(lowValue))
		{
			est.setEdges(null);
			playSheet.getView().repaint();
			return;
		}
		Enumeration enumKey = edgeWeightHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			double value = (Double) edgeWeightHash.get(key);
			//if value equal to 0, dont need to calculate
			if(value != 0.0)
			{
				value = ((value -lowValue)/(highValue-lowValue))*multiplier;
			}
			edgeWeightHash.put(key,  value);
		}

		est.setEdges(edgeWeightHash);
		playSheet.getView().repaint();

	}


}
