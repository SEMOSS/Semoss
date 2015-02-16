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
package prerna.ui.components;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.Shape;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;

/**
 * This class is used to create the legend for visualizations.
 */
public class LegendPanel2 extends JPanel {
	
	static final Logger logger = LogManager.getLogger(LegendPanel2.class.getName());

	public ArrayList<PaintLabel> icons;
	public ArrayList<JLabel> labels;
	public ArrayList<JPanel> panels;
	public ArrayList<String> labelnames;
	JPanel panel;
	
	public VertexFilterData data = null;

	/**
	 * Create the panel.
	 */
	public LegendPanel2() {
		setLayout(new BorderLayout(0, 0));
		
		panel = new JPanel();
		panel.setToolTipText("You can adjust the shape and color by going to the cosmetics tab on the navigation panel");

//		panel.setMaximumSize(new Dimension(500, 40));
//		scrollPane.setViewportView(panel);
		panel.setLayout(new WrapLayout(WrapLayout.LEFT,15,0));
		add(panel,BorderLayout.CENTER);

		icons = new ArrayList<PaintLabel>();
		labels = new ArrayList<JLabel>();
		panels = new ArrayList<JPanel>();
		labelnames = new ArrayList<String>();
	}
	
	/**
	 * Sets the vertex filter data.
	 * @param data VertexFilterData
	 */
	public void setFilterData(VertexFilterData data)
	{
		this.data = data;
	}
	
	/**
	 * This method will draw the legend for visualizations.
	 */
	public void drawLegend()
	{
		// this will draw the legend
		// get the type hash
		// for each node type - find what is the color and shape
		// paint it
		// specify the node type
		Hashtable <String, Vector> nodeHash = data.typeHash;

		int count =0;
		while(count<labelnames.size())
		{
			String label = labelnames.get(count);
			if(nodeHash.containsKey(label))
				count++;
			else
			{
				JPanel toRemove = panels.remove(count);
				panel.remove(toRemove);
				labelnames.remove(count);
			}
		}

		try {
			Enumeration <String> nodeTypes = nodeHash.keys();
			for(int idx = 0;nodeTypes.hasMoreElements();idx++)
			{
				String nodeType = nodeTypes.nextElement();
				int index = labelnames.indexOf(nodeType);
				if(index<0)
				{
					labelnames.add(nodeType);					
					JPanel panel1 = new JPanel();
					panel1.setLayout(new BoxLayout(panel1, BoxLayout.Y_AXIS));
					
					PaintLabel icon1 = new PaintLabel("");
					icon1.setPreferredSize(new Dimension(20, 20));
					icon1.setMinimumSize(new Dimension(20, 20));
					icon1.setMaximumSize(new Dimension(20, 20));
					icon1.setAlignmentX(CENTER_ALIGNMENT);
					panel1.add(icon1);

					JLabel label1 = new JLabel("");
					label1.setAlignmentX(CENTER_ALIGNMENT);
					panel1.add(label1);
					panel1.setAlignmentX(LEFT_ALIGNMENT);
					
					panel.add(panel1);
					panels.add(panel1);
					index = panels.size()-1;				
					
				}
				
//				PaintLabel pl = (PaintLabel)iconField.get(this);
				PaintLabel pl = (PaintLabel) panels.get(index).getComponent(0);
				Vector <SEMOSSVertex> vector = nodeHash.get(nodeType);
				SEMOSSVertex vert = vector.elementAt(0);
				int typeSize = vector.size();
				
				Method method = PaintLabel.class.getMethod("setShape", Shape.class);
				Method method2 = PaintLabel.class.getMethod("setColor", Color.class);
				Method method3 = JLabel.class.getMethod("setToolTipText", String.class);
				Shape shape = TypeColorShapeTable.getInstance().getShapeL(vert.getProperty(Constants.VERTEX_TYPE)+"", vert.getProperty(Constants.VERTEX_NAME) +"");
				Color color = vert.getColor();
				method.invoke(pl, shape);
				method2.invoke(pl, color);
				method3.invoke(pl, nodeType+"("+typeSize+")");
				
//				JLabel tl = (JLabel)labelField.get(this);
				JLabel tl = (JLabel) panels.get(index).getComponent(1);
				Method method4 = JLabel.class.getMethod("setText", String.class);
				Method method5 = JLabel.class.getMethod("setToolTipText", String.class);
				logger.info("Setting the text to " + nodeType);
				method4.invoke(tl, nodeType+"("+typeSize+")");
				method5.invoke(tl, nodeType+"("+typeSize+")");
				
			}

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		
		// iterate through the nodes and make modifications
		
		// for the remaining ones, get rid of it labels
		logger.info("panelsize"+panel.size());
		logger.info("bottom of frame size"+this.size());
	}
}
