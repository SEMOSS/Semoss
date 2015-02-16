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

import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.playsheets.GraphPlaySheet;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;


/**
 * This class is used to configure menu items in the layout.
 */
public class LayoutMenuItem extends JMenuItem{
	IPlaySheet ps = null;
	static final Logger logger = LogManager.getLogger(LayoutMenuItem.class.getName());
	String layout = null;
	
	/**
	 * Constructor for LayoutMenuItem.
	 * @param layout String
	 * @param ps IPlaySheet
	 */
	public LayoutMenuItem(String layout, IPlaySheet ps)
	{
		super(layout);
		this.ps = ps;
		this.layout = layout;
	}
	
	/**
	 * Paints the specified layout.
	 */
	public void paintLayout()
	{
		String oldLayout = ((GraphPlaySheet)ps).getLayoutName();
		((GraphPlaySheet)ps).setLayout(layout);
		boolean success = ((GraphPlaySheet)ps).createLayout();
		if (success) ((GraphPlaySheet)ps).refreshView();
		else {
			if(layout.equals(Constants.RADIAL_TREE_LAYOUT) || layout.equals(Constants.BALLOON_LAYOUT) || layout.equals(Constants.TREE_LAYOUT)){
				int response = showOptionPopup();
				if (response ==1)
				{
					((GraphPlaySheet)ps).searchPanel.treeButton.doClick();
				}
				else{
					((GraphPlaySheet)ps).setLayout(oldLayout);
				}
			}
			else{
				Utility.showError("This layout cannot be used with the current graph");
				((GraphPlaySheet)ps).setLayout(oldLayout);
			}
		}
	}
	/**
	 * This displays options to the user in a popup menu about what type of layout they want to display.
	
	 * @return int 	User response. */
	private int showOptionPopup(){
		JFrame playPane = (JFrame) DIHelper.getInstance().getLocalProp(Constants.MAIN_FRAME);
		Object[] buttons = {"Cancel Layout", "Continue With Tree"};
		int response = JOptionPane.showOptionDialog(playPane, "This layout requires the graph to be in the format of a tree.\nWould you like to duplicate nodes so that it is in the fromat of a tree?\n\nPreferred root node must already be selected", 
				"Convert to Tree", JOptionPane.YES_NO_OPTION, JOptionPane.QUESTION_MESSAGE, null, buttons, buttons[1]);
		return response;
	}
}
