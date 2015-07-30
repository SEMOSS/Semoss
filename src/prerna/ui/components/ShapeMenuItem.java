/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
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

import javax.swing.JMenuItem;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.util.Constants;
import prerna.util.QuestionPlaySheetStore;


/**
 * This class is used to create a menu item for different shapes.
 */
public class ShapeMenuItem extends JMenuItem{
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	String shape = null;
	static final Logger logger = LogManager.getLogger(ShapeMenuItem.class.getName());

	/**
	 * Constructor for ShapeMenuItem.
	 * @param shape String
	 * @param ps IPlaySheet
	 * @param pickedVertex DBCMVertex[]
	 */
	public ShapeMenuItem(String shape, IPlaySheet ps, SEMOSSVertex [] pickedVertex)
	{
		super(shape);
		this.ps = ps;
		this.shape = shape;
		this.pickedVertex = pickedVertex;
	}
	
	/**
	 * Paints the shape.
	 */
	public void paintShape()
	{
		TypeColorShapeTable tcst = TypeColorShapeTable.getInstance();
		for(int vertIndex = 0;vertIndex < pickedVertex.length;vertIndex++)
		{
			tcst.addShape(""+pickedVertex[vertIndex].getProperty(Constants.VERTEX_NAME), shape);
		}
		GraphPlaySheet playSheet = (GraphPlaySheet) QuestionPlaySheetStore.getInstance().getActiveSheet();
		playSheet.repaint();
	}
}
