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

import javax.swing.DefaultListModel;
import javax.swing.JList;
import javax.swing.event.ListSelectionEvent;
import javax.swing.event.ListSelectionListener;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.GraphToTreeConverter;
import prerna.ui.components.WeightDropDownButton;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.transformer.VertexShapeTransformer;

/**
 * 
 */
public class WeightConvertNodeListListener implements ListSelectionListener{

	GraphPlaySheet playSheet;
	GraphToTreeConverter converter;
	WeightDropDownButton btnMenu;
	double minimumValue =.5;
	double multiplier = 3;
	
	/**
	 * Constructor for WeightConvertNodeListListener.
	 */
	public WeightConvertNodeListListener(){
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
		((DefaultListModel) list.getModel()).contains(prop);
		Double highValue = null;
		Double lowValue = null;
		Collection<SEMOSSVertex> nodeArray = playSheet.forest.getVertices();
		Hashtable nodeWeightHash = new Hashtable();
		VertexShapeTransformer vst = (VertexShapeTransformer)playSheet.getView().getRenderContext().getVertexShapeTransformer();
		
		//if prop is null deselection happened and we will just reset everything
		if(prop ==null)
		{
			vst.setVertexSizeHash(new Hashtable());
			playSheet.getView().repaint();
			return;
		}
		for (SEMOSSVertex node: nodeArray)
		{			
			if(node.propHash.containsKey(prop) && node.propHash.get(prop) instanceof Double)
			{
					
				double value = (Double)node.propHash.get(prop);
				if(highValue ==null)
					highValue = value;
				if (lowValue == null)
					lowValue = value;
				if (value>highValue)
					highValue = value;
				if (value<lowValue)
					lowValue = value;
				
				nodeWeightHash.put(node.getURI(), value);
			}
		}

		if(highValue==null || lowValue==null || highValue.equals(lowValue))
		{
			vst.setVertexSizeHash(new Hashtable());
			playSheet.getView().repaint();
			return;
		}
		
		double currentWeight = vst.getDefaultScale();
		Enumeration enumKey = nodeWeightHash.keys();
		while (enumKey.hasMoreElements())
		{
			String key = (String) enumKey.nextElement();
			double value = (Double) nodeWeightHash.get(key);
			//if value equal to 0, dont need to calculate
			if(value != 0.0)
			{
				value = ((value -lowValue)/(highValue-lowValue))*multiplier*currentWeight+minimumValue;
			}
			nodeWeightHash.put(key,  value);
		}
	
		vst.setVertexSizeHash(nodeWeightHash);
		playSheet.getView().repaint();
	}
}
