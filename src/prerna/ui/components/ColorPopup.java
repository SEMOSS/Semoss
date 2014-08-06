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
import prerna.ui.main.listener.impl.ColorMenuListener;

/**
 * This class sets the visualization viewer for a popup menu.
 */
public class ColorPopup extends JMenu{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	// sets the picked node list
	SEMOSSVertex [] pickedVertex = null;
	static final Logger logger = LogManager.getLogger(ColorPopup.class.getName());
	/**
	 * Constructor for ColorPopup.
	 * @param name String
	 * @param ps IPlaySheet
	 * @param pickedVertex DBCMVertex[]
	 */
	public ColorPopup(String name, IPlaySheet ps, SEMOSSVertex [] pickedVertex)
	{
		super(name);
		// need to get this to read from popup menu
		this.ps = ps;
		this.pickedVertex = pickedVertex;
		showColors();
	}
	
	/**
	 * Gets all the colors for a playsheet from the type color shape table.
	 * Loops through the colors and adds menu item.
	 */
	public void showColors()
	{
		TypeColorShapeTable tcst = TypeColorShapeTable.getInstance();
		String [] colors = tcst.getAllColors();
		
		for(int colorIndex = 0;colorIndex < colors.length;colorIndex++)
		{
			ColorMenuItem item = new ColorMenuItem(colors[colorIndex], ps, pickedVertex);
			add(item);
			item.addActionListener(ColorMenuListener.getInstance());
		}
	}
	}
