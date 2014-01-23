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

import javax.swing.JFrame;
import javax.swing.JMenuItem;
import javax.swing.JOptionPane;

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
	Logger logger = Logger.getLogger(getClass());
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
