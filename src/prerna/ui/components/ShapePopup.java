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
package prerna.ui.components;

import javax.swing.JMenu;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.om.SEMOSSVertex;
import prerna.ui.components.api.IPlaySheet;
import prerna.ui.helpers.TypeColorShapeTable;
import prerna.ui.main.listener.impl.ShapeMenuListener;

/**
 * This class is used to display information about shapes in a popup menu.
 */
public class ShapePopup extends JMenu{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(ShapePopup.class.getName());
	/**
	 * Constructor for ShapePopup.
	 * @param name String
	 * @param ps IPlaySheet
	 * @param pickedVertex DBCMVertex[]
	 */
	public ShapePopup(String name, IPlaySheet ps, SEMOSSVertex [] pickedVertex)
	{
		super(name);
		// need to get this to read from popup menu
		this.ps = ps;
		this.pickedVertex = pickedVertex;
		showShapes();
	}
	
	/**
	 * Shows the available different shapes in the popup menu.
	 */
	public void showShapes()
	{
		TypeColorShapeTable tcst = TypeColorShapeTable.getInstance();
		String [] shapes = tcst.getAllShapes();
		
		for(int colorIndex = 0;colorIndex < shapes.length;colorIndex++)
		{
			ShapeMenuItem item = new ShapeMenuItem(shapes[colorIndex], ps, pickedVertex);
			add(item);
			item.addActionListener(ShapeMenuListener.getInstance());
		}
	}
	}
