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
