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
			if(edge.propHash.containsKey(prop) && edge.propHash.get(prop) instanceof Double)
			{
					
				double value = (Double)edge.propHash.get(prop);
				if(highValue ==null)
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
