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
package prerna.ui.helpers;

import java.awt.Color;
import java.awt.Shape;
import java.util.Hashtable;
import java.util.Random;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.util.Constants;
import prerna.util.DIHelper;

/**
 * This is the table that stores all the shapes and colors for nodes on the graph play sheet.
 */
public class TypeColorShapeTable {
	Hashtable <String, Shape> shapeHash = new Hashtable<String, Shape>();
	Hashtable <String, Color> colorHash = new Hashtable<String, Color>();

	Hashtable <String, Shape> shapeHashL = new Hashtable<String, Shape>();

	Hashtable <String, String> shapeStringHash = new Hashtable<String, String>();
	Hashtable <String, String> colorStringHash = new Hashtable<String, String>();

	String [] shapes = null;
	String [] colors = null;
	static TypeColorShapeTable instance = null;
	
	static final Logger logger = LogManager.getLogger(TypeColorShapeTable.class.getName());
	
	/**
	 * Constructor for TypeColorShapeTable.
	 */
	protected TypeColorShapeTable()
	{
		
	}
	
	/**
	 * Method getInstance.  Gets an instance of a TypeColorShapeTable.
	
	 * @return TypeColorShapeTable */
	public static TypeColorShapeTable getInstance()
	{
		if(instance == null)
		{
			instance = new TypeColorShapeTable();
			instance.getAllShapes();
			instance.getAllColors();
		}
		return instance;
	}
	
	/**
	 * Method clearAll.  Clears all information from the shape and color hashtables.
	 */
	public void clearAll()
	{
		shapeHash.clear();
		shapeHashL.clear();
		colorHash.clear();
		shapeStringHash.clear();
		colorStringHash.clear();
	}
	
	/**
	 * Method getAllShapes. Gets all the shapes from the instance of the shapes array.
	 * Sets to default shape list if there is no instance.
	
	 * @return String[] - An array containing all the shapes.*/
	public String [] getAllShapes()
	{
		if(shapes == null)
		{
			shapes = new String[7];
			shapes[0] = Constants.TRIANGLE;
			shapes[1] = Constants.CIRCLE;
			shapes[2] = Constants.SQUARE;
			shapes[3] = Constants.DIAMOND;
			shapes[4] = Constants.STAR;
			shapes[5] = Constants.PENTAGON;
			shapes[6] = Constants.HEXAGON;
			
		}
		return shapes;
	}

	/**
	 * Method getAllColors.  Gets all the colors from the instance of the shapes array.
	 * Sets to default color list if there is no instance.
	
	 * @return String[] - An array containing all the colors.*/
	public String [] getAllColors()
	{
		if(colors == null)
		{
			colors = new String[10];
			colors[0] = Constants.BLUE;
			colors[1] = Constants.GREEN;
			colors[2] = Constants.RED;
			colors[3] = Constants.BROWN;
			colors[4] = Constants.MAGENTA;
			colors[5] = Constants.ORANGE;
			colors[6] = Constants.YELLOW;
			colors[7] = Constants.AQUA;
			colors[8] = Constants.PURPLE;
			colors[9] = Constants.TRANSPARENT;
			
		}
		return colors;
	}

	
	/**
	 * Method addShape. Adds a shape to the local Hashtable of shapes.  Gets the shape from DI Helper.
	 * @param type String - The type of the shape (serves as a key in the hashtable)
	 * @param shape String - the shape itself.
	 */
	public void addShape(String type, String shape)
	{
		Shape thisShape = (Shape)DIHelper.getInstance().getLocalProp(shape);
		Shape thisShapeL = (Shape)DIHelper.getInstance().getLocalProp(shape+Constants.LEGEND);
		shapeStringHash.put(type, shape);
		shapeHash.put(type, thisShape);
		shapeHashL.put(type, thisShapeL);
	}
	
	/**
	 * Method getColor.  Gets the color from the color Hashtable.
	 * @param name String - name of the color
	
	 * @return Color - Retrieved color from the hashtable*/
	public Color getColor(String name)
	{
		return colorHash.get(name);
	}
	
	/**
	 * Method addColor.  Adds a color to the local Hashtable of colors.  Gets the shape from DI Helper.
	 * @param type String - the type of the shape
	 * @param color String - the color of the shape
	 */
	public void addColor(String type, String color)
	{
		// get the shape from DIHelper
		if(DIHelper.getInstance().getLocalProp(color) != null)
		{
			Color thisColor = (Color)DIHelper.getInstance().getLocalProp(color);
			colorHash.put(type, thisColor);
		}
		colorStringHash.put(type, color);
	}

	/**
	 * Method getShape.  Gets the shape of a node based on the parameters
	 * @param type String - the type of the node
	 * @param vertName String - the name of the vertex
	
	 * @return Shape - the shape of the node*/
	public Shape getShape(String type, String vertName)
	{
		logger.debug("Getting type / vertex name " + type + " <> " + vertName);
		Shape retShape = null;
		// first check the vertex name
		if(shapeHash.containsKey(vertName))
			retShape = shapeHash.get(vertName);
		else if(shapeHash.containsKey(type))
			retShape = shapeHash.get(type);
		else if(DIHelper.getInstance().getProperty(type+"_SHAPE") != null)
			// try to search the properties file for the first time
			retShape = (Shape)DIHelper.getInstance().getLocalProp(DIHelper.getInstance().getProperty(type+"_SHAPE"));
		if(retShape == null){
			//instead of returning default color, going to return random color that hasn't been used yet
			for(String shape : shapes){
				if(!shapeStringHash.containsValue(shape)){
					//got a unique color, set node that color
					retShape = (Shape) DIHelper.getInstance().getLocalProp(shape);
					addShape(type, shape);
					break;
				}
			}
			if (retShape == null){
				//if all of the colors have already been used, just grab a random color
		        Object[] keys = shapeHash.keySet().toArray();
		        Object key = keys[new Random().nextInt(keys.length)];
		        retShape = shapeHash.get(key);
		        String shapeString = shapeStringHash.get(key);
				addShape(type, shapeString);
			}
		}
		//logger.info("Shape for Type " + type + "[][]" + retShape);
		//shapeHash.put(type, retShape);
		return retShape;
	}
	
	/**
	 * Method getShape.  Gets the shape of a node based on the parameters and the legend.
	 * @param type String - the type of the node
	 * @param vertName String - the name of the vertex
	
	 * @return Shape - the shape of the node*/
	public Shape getShapeL(String type, String vertName)
	{
		logger.debug("Getting type / vertex name " + type + " <> " + vertName);
		Shape retShape = null;
		// first check the vertex name
		if(shapeHashL.containsKey(vertName))
			retShape = shapeHashL.get(vertName);
		else if(shapeHashL.containsKey(type))
			retShape = shapeHashL.get(type);
		else if(DIHelper.getInstance().getProperty(type+"_SHAPE") != null)
			// try to search the properties file for the first time
			retShape = (Shape)DIHelper.getInstance().getLocalProp(DIHelper.getInstance().getProperty(type+"_SHAPE") + Constants.LEGEND);
		if(retShape == null){
			//instead of returning default color, going to return random color that hasn't been used yet
			for(String shape : shapes){
				if(!shapeStringHash.containsValue(shape)){
					//got a unique color, set node that color
					retShape = (Shape) DIHelper.getInstance().getLocalProp(shape+Constants.LEGEND);
					addShape(type, shape);
					break;
				}
			}
			if (retShape == null){
				//if all of the colors have already been used, just grab a random color
		        Object[] keys = shapeHashL.keySet().toArray();
		        Object key = keys[new Random().nextInt(keys.length)];
		        retShape = shapeHashL.get(key);
		        String shapeString = shapeStringHash.get(key);
				addShape(type, shapeString);
			}
		}

		//logger.info("Shape for Type " + type + "[][]" + retShape);
		//shapeHash.put(type, retShape);
		return retShape;
		
	}
	
	/**
	 * Method getColor.  Gets the color based on the parameters.
	 * @param type String - the type of node
	 * @param vertName String - vertex name
	
	 * @return Color - the color based on the type and vertex name*/
	public Color getColor(String type, String vertName)
	{
		Color retColor = null;
		// first check the vertex name
		if(colorHash.containsKey(vertName))
			retColor = colorHash.get(vertName);
		else if(colorHash.containsKey(type))
			retColor = colorHash.get(type);
		else if(DIHelper.getInstance().getProperty(type+"_COLOR") != null)
			// try to search the properties file for the first time
			retColor = (Color)DIHelper.getInstance().getLocalProp(DIHelper.getInstance().getProperty(type+"_COLOR"));
		if(retColor == null && !colorStringHash.containsKey(vertName)){
			//instead of returning default color, going to return random color that hasn't been used yet
			for(String color : colors){
				if(!colorStringHash.containsValue(color)){
					//got a unique color, set node that color
					retColor = (Color) DIHelper.getInstance().getLocalProp(color);
					addColor(type, color);
					break;
				}
			}
			if (retColor == null){
				//if all of the colors have already been used, just grab a random color
		        Object[] keys = colorHash.keySet().toArray();
		        Object key = keys[new Random().nextInt(keys.length)];
		        retColor = colorHash.get(key);
		        String colorString = colorStringHash.get(key);
				addColor(type, colorString);
			}
		}
		if(colorStringHash.containsKey(vertName) && colorStringHash.get(vertName).equalsIgnoreCase(Constants.TRANSPARENT))
			retColor = null;
		//colorHash.put(type, retColor);
		return retColor;
	}
	
	/**
	 * Method getShapeAsString. - Retrieves the shape in string format from the shapeString hashtable.
	 * @param vertName String - vertex name - the key for the desired shape in the hashtable
	
	 * @return String - the shape in string format */
	public String getShapeAsString(String vertName)
	{
		if(shapeStringHash.containsKey(vertName))
			return shapeStringHash.get(vertName);
		else
			return "TBD";
	}
	/**
	 * Method getColorAsString.  Retrieves the color in string format from the colorString hashtable,
	 * @param vertName String - vertex name - the key for the desired color in the hashtable
	
	 * @return String - the Color in string format. */
	public String getColorAsString(String vertName)
	{
		if(colorStringHash.containsKey(vertName))
			return colorStringHash.get(vertName);
		else
			return "TBD";
	}

}
