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
package prerna.ui.components;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.PropComparator;
import edu.uci.ics.jung.visualization.VisualizationViewer;

/**
 * This class is used to keep track of all the properties within the tool.
 */
public class ControlData{
	
	// for a given types
	// keeps all the properties available
	// key is the vertex type
	// values are all of the properties within this vertex
	Hashtable <String, Hashtable> properties = new Hashtable<String, Hashtable>();
	ArrayList <String> selectedProperties = new ArrayList<String>();
	
	VisualizationViewer viewer = null;

	// this will be utilized for tooltips
	Object [][] toolTipRows = null;
	// this will be utilized for labels
	Object [][] labelRows = null;
	// this will be utilized for color
	Object [][] colorRows = null;
	// this will be utilized for edge color
	Object [][] edgeThicknessRows = null;
	
	// this is utilized for all the labels
	Hashtable <String, Vector> typePropertySelectedList = new Hashtable<String, Vector>();

	// this is utilized for all the labels
	Hashtable <String, String> typePropertyUnSelectedList = new Hashtable<String, String>();

	// this would be utilized for tooltips
	Hashtable <String, Vector> typePropertySelectedListTT = new Hashtable<String, Vector>();
	
	// this would be utilized for tooltips
	Hashtable <String, String> typePropertyUnSelectedListTT = new Hashtable<String, String>();
	
	int rowCount = 0;
	
	// these are the properties that are always on
	Hashtable <String, String> propOn = new Hashtable<String, String>();

	// these are the properties that are always hidden
	Hashtable <String, String> propHide = new Hashtable<String, String>();
	
	// these are the properties that are always on
	Hashtable <String, String> propOnT = new Hashtable<String, String>();
	
	static final Logger logger = LogManager.getLogger(ControlData.class.getName());
	
	/**
	 * Constructor for ControlData.
	 */
	public ControlData()
	{
		// put what we want to show first in all of these things
		propOn.put(Constants.VERTEX_NAME, Constants.VERTEX_NAME);
		propOnT.put(Constants.EDGE_NAME, Constants.EDGE_NAME);
		propOnT.put(Constants.EDGE_TYPE, Constants.EDGE_TYPE);
		propOnT.put(Constants.URI, Constants.URI);
		propOnT.put(Constants.VERTEX_NAME, Constants.VERTEX_NAME);
		propOnT.put(Constants.VERTEX_TYPE, Constants.VERTEX_TYPE);
		propHide.put(Constants.VERTEX_COLOR, Constants.VERTEX_COLOR);
	}

	/**
	 * Adds a property of a specific type to the property hashtable.
	 * @param type 		Type of property.
	 * @param property 	Property.
	 */
	public void addProperty(String type, String property)
	{
		logger.debug("Adding " + type + "<>" + property);
		Hashtable <String, String> typeProp = new Hashtable<String,String>();
		if(properties.containsKey(type))
			typeProp = properties.get(type);
		if(!typeProp.containsKey(property) && !propHide.containsKey(property))
		{
			typeProp.put(property, property);
			rowCount++;
			logger.debug("Adding " + type + "<>" + property);
		}
		properties.put(type, typeProp);
	}
	
	
	
	/**
	 * Generates all the rows in the control panel.
	 */
	public void generateAllRows()
	{
		// clear everything first
		//typePropertySelectedList.clear();
		//typePropertySelectedListTT.clear();
		
		
		// columns are - Type - Property - Boolean
		toolTipRows = new Object[rowCount+1][4];
		labelRows = new Object[rowCount+1][4];
		
		//Add select all rows
		labelRows[0][0] = "SELECT ALL";
		labelRows[0][1] = "";
		labelRows[0][2] = new Boolean(true);
		labelRows[0][3] = "SELECT ALL";
		toolTipRows[0][0] = "SELECT ALL";
		toolTipRows[0][1] = "";
		toolTipRows[0][2] = new Boolean(true);
		toolTipRows[0][3] = "SELECT ALL";
		
		// Color Rows and have columns
		// Type - Color
		colorRows = new Object[properties.size()][2];
		
		// edge thickness
		// type - Thickness
		edgeThicknessRows = new Object[properties.size()][2];
		
		ArrayList <String> types = new ArrayList(properties.keySet());
		Collections.sort(types);
		int rowIndex = 1;
		for(int typeIndex=0;typeIndex<types.size();typeIndex++)
		{
			String type = types.get(typeIndex);
			colorRows[typeIndex][0] = type;
			colorRows[typeIndex][1] = "TBD";

			edgeThicknessRows[typeIndex][0] = type;
			edgeThicknessRows[typeIndex][1] = "TBD";
			// get the next hashtable now
			Hashtable propTable = properties.get(type);
			ArrayList <String> props = new ArrayList(propTable.keySet());
			Collections.sort(props, new PropComparator());
			for(int propIndex=0;propIndex<props.size();propIndex++)	
			{
				String prop = props.get(propIndex);
				if(!propHide.containsKey(prop)){
					if(propIndex == 0)
						toolTipRows[rowIndex][0] = type;
					else
						toolTipRows[rowIndex][0] = "";
	
					toolTipRows[rowIndex][1] = prop;
					
					boolean preSelectedT = typePropertyUnSelectedListTT.containsKey(type);
					
					boolean foundPropT = findIfPropSelected(typePropertySelectedListTT,type, prop);
					toolTipRows[rowIndex][2] = new Boolean(foundPropT);
					
					if((propOnT.containsKey(prop) && !preSelectedT) && !foundPropT)
					{
						toolTipRows[rowIndex][2] = new Boolean(true);
						Vector <String> typePropList = new Vector<String>();
						if(typePropertySelectedListTT.containsKey(type))
							typePropList = typePropertySelectedListTT.get(type);
						typePropList.addElement(toolTipRows[rowIndex][1]+"");
						typePropertySelectedListTT.put(type, typePropList);
					}
					toolTipRows[rowIndex][3] = type;
					
	
					if(propIndex == 0)
						labelRows[rowIndex][0] = type;
					else
						labelRows[rowIndex][0] = "";
					
					labelRows[rowIndex][1] = prop;
					
					boolean foundProp = findIfPropSelected(typePropertySelectedList,type, prop);
					
					labelRows[rowIndex][2] = new Boolean(foundProp);
					
					boolean preSelected = typePropertyUnSelectedList.containsKey(type);
					
					if((propOn.containsKey(prop) && !preSelected) && !foundProp)
					{
						labelRows[rowIndex][2] = new Boolean(true);
						Vector <String> typePropList2 = new Vector<String>();
						if(typePropertySelectedList.containsKey(type))
							typePropList2 = typePropertySelectedList.get(type);
						typePropList2.add(prop);
						typePropertySelectedList.put(type, typePropList2);
					}
					
					labelRows[rowIndex][3] = type;
					logger.debug("Adding Rows -- " + rowIndex + "<>" + type + "<>" + prop);
					rowIndex++;
				}
			}
		}
		//propOn.clear();
	}
	
	/**
	 * Checks if property for a certain type is selected.
	 * @param list	List of properties.
	 * @param type 	Property type.
	 * @param prop	Property.
	
	 * @return boolean	True if a property is selected. */
	private boolean findIfPropSelected(Hashtable <String, Vector> list, String type, String prop)
	{
		logger.debug("Trying to see if property " + prop + " for type " + type +  " is selected");
		boolean retBool = false;
		if(list.containsKey(type))
		{
			Vector <String> typePropList2 = list.get(type);
			for(int propIndex = 0;propIndex < typePropList2.size() && !retBool;propIndex++)
				if(typePropList2.elementAt(propIndex).equalsIgnoreCase(prop))
					retBool = true;
		}
		logger.debug(prop +" selection is " + retBool + "  for type " + type);
		return retBool;
	}

//	/**
//	 * Checks if property for a certain type is selected.
//	 * @param list	List of properties.
//	 * @param type 	Property type.
//	 * @param prop	Property.
//	
//	 * @return boolean	True if a property is selected. */
//	private boolean findIfPropUnSelected(Hashtable <String, Vector> list, String type, String prop)
//	{
//		logger.debug("Trying to see if property " + prop + " for type " + type +  " is selected");
//		boolean retBool = false;
//		if(list.containsKey(type))
//		{
//			Vector <String> typePropList2 = list.get(type);
//			for(int propIndex = 0;propIndex < typePropList2.size() && !retBool;propIndex++)
//				if(typePropList2.elementAt(propIndex).equalsIgnoreCase(prop))
//					retBool = true;
//		}
//		logger.debug(prop +" selection is " + retBool + "  for type " + type);
//		return retBool;
//	}

	/**
	 * Gets label value from a particular row and column location.
	 * @param row 		Row number.
	 * @param column 	Column number.
	
	 * @return Object 	Label value. */
	public Object getLabelValueAt(int row, int column)
	{
		logger.debug(" Trying to return values - Label " + row + "<>" + column + "<>" + labelRows[row][column]);
		return labelRows[row][column];
	}

	/**
	 * Sets label value at a particular row and column location.
	 * @param val 		Label value.
	 * @param row 		Row number.
	 * @param column 	Column number.
	 */
	public void setLabelValueAt(Object val, int row, int column)
	{
		//check if it is the header row--select all
		if(row==0){
			for (int rowIdx = 1; rowIdx<labelRows.length; rowIdx++)
				setLabelValueAt(val, rowIdx, 2);
		}
		String type = labelRows[row][3]+"";
		Vector <String> typePropList = new Vector<String>();
		Vector <String> typeUnPropList = new Vector<String>();

		if(typePropertySelectedList.containsKey(type))
			typePropList = typePropertySelectedList.get(type);
		if(val instanceof Boolean)
		{
			if((Boolean)val)
			{
				typePropList.addElement(labelRows[row][1]+"");
			}
			else
			{
				typePropList.removeElement(labelRows[row][1]+"");
				typePropertyUnSelectedList.put(type, type);
			}
		}
		labelRows[row][column] = val;
		typePropertySelectedList.put(type, typePropList);
		viewer.repaint();
	}

	/**
	 * Gets tooltip value at a particular row and column location.
	 * @param row 		Row number.
	 * @param column 	Column number.
	
	 * @return Object 	Tooltip value. */
	public Object getTooltipValueAt(int row, int column)
	{
		return toolTipRows[row][column];
	}

	/**
	 * Sets tooltip value at a particular row and column location.
	 * @param val 		Tooltip value.
	 * @param row 		Row number.
	 * @param column 	Column number.
	 */
	public void setTooltipValueAt(Object val, int row, int column)
	{
		//check if it is the header row--select all
		if(row==0){
			for (int rowIdx = 1; rowIdx<toolTipRows.length; rowIdx++)
				setTooltipValueAt(val, rowIdx, 2);
		}
		String type = toolTipRows[row][3]+"";
		Vector <String> typePropList = new Vector<String>();
		if(typePropertySelectedListTT.containsKey(type))
			typePropList = typePropertySelectedListTT.get(type);
		if(val instanceof Boolean)
		{
			logger.debug("Value is currently boolean " + val);
			if((Boolean)val)
				typePropList.addElement(toolTipRows[row][1]+"");
			else{
				typePropList.removeElement(toolTipRows[row][1]+"");
				typePropertyUnSelectedListTT.put(type, type);
			}
		}
		toolTipRows[row][column] = val;
		typePropertySelectedListTT.put(type, typePropList);
		viewer.repaint();
	}

	/**
	 * Gets the color value from a particular row and column.
	 * @param row 		Row number.
	 * @param column 	Column number.
	
	 * @return Object 	Color value.*/
	public Object getColorValueAt(int row, int column)
	{
		return colorRows[row][column];
	}

	/**
	 * Sets the color value at a particular row and column.
	 * @param val 		Color value.
	 * @param row		Row number.
	 * @param column 	Column number.
	 */
	public void setColorValueAt(Object val, int row, int column)
	{
		colorRows[row][column] = val;
	}

	/**
	 * Gets the thickness of an edge of a particular row and column.
	 * @param row		Row number.
	 * @param column 	Column number.
	
	 * @return Object	Edge thickness value. */
	public Object getEdgeThicknessValueAt(int row, int column)
	{
		return edgeThicknessRows[row][column];
	}

	/**
	 * Sets the thickness of an edge at a particular row and column.
	 * @param val		Thickness value.
	 * @param row		Row number.
	 * @param column	Column number.
	 */
	public void setThicknessValueAt(Object val, int row, int column)
	{
		edgeThicknessRows[row][column] = val;
	}
	
	/**
	 * Gets properties of a specific type.
	 * @param type 	Type of property to retrieve.
	
	 * @return Vector<String> 	List of properties. */
	public Vector<String> getSelectedProperties(String type)
	{
		return typePropertySelectedList.get(type);
	}

	/**
	 * Gets all of the tooltip specific properties.
	 * @param type String	Type of property to retrieve.
	
	 * @return Vector<String>	List of properties. */
	public Vector<String> getSelectedPropertiesTT(String type)
	{
		return typePropertySelectedListTT.get(type);
	}

	/**
	 * Sets the viewer.
	 * @param viewer VisualizationViewer
	 */
	public void setViewer(VisualizationViewer viewer)
	{
		this.viewer = viewer;
	}
}
