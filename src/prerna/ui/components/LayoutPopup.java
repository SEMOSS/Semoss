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

import javax.swing.JMenu;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;
import prerna.ui.main.listener.impl.LayoutMenuListener;
import prerna.util.Constants;

/**
 * This class is used to create a popup that allows the user to pick the layout.
 */
public class LayoutPopup extends JMenu{
	
	// need a way to cache this for later
	// sets the visualization viewer
	IPlaySheet ps = null;
	String [] layoutNames = {Constants.FR, Constants.KK, Constants.SPRING, Constants.ISO, Constants.CIRCLE_LAYOUT, Constants.TREE_LAYOUT, Constants.RADIAL_TREE_LAYOUT, Constants.BALLOON_LAYOUT};
	
	static final Logger logger = LogManager.getLogger(LayoutPopup.class.getName());
	/**
	 * Constructor for LayoutPopup.
	 * @param name String
	 * @param ps IPlaySheet
	 */
	public LayoutPopup(String name, IPlaySheet ps)
	{
		super(name);
		// need to get this to read from popup menu
		this.ps = ps;
		showLayouts();
	}
	
	/**
	 * Shows the different layouts available for the display.
	 */
	public void showLayouts()
	{
		for(int layoutIndex = 0;layoutIndex < layoutNames.length;layoutIndex++)
		{
			LayoutMenuItem item = new LayoutMenuItem(layoutNames[layoutIndex], ps);
			add(item);
			item.addActionListener(LayoutMenuListener.getInstance());
		}
	}
	}
