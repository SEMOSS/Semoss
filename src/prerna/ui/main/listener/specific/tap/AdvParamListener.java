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
package prerna.ui.main.listener.specific.tap;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.JToggleButton;

import prerna.ui.components.specific.tap.SerOptPlaySheet;
import prerna.ui.components.specific.tap.SysOptPlaySheet;

/**
 * Listener to show advanced parameters for TAP optimization
 * Used for when showParamBtn, showSystemSelectBtn, or showSystemCapSelectBtn
 * is pressed on the TAP service optimization or TAP system optimization playsheet
 */
public class AdvParamListener implements ActionListener {
	
	SerOptPlaySheet ps;
	JToggleButton showParamBtn, showSystemSelectBtn, showSystemCapSelectBtn;
	boolean isSysOpt;
	
	/**
	 * Action for when showParamBtn, showSystemSelectBtn, or showSystemCapSelectBtn is pressed
	 * on the TAP service or TAP system optimization playsheet
	 * When pressed, makes advParamPanel become visible or system/capability panel visible
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e) {
		if(e.getSource().equals(showParamBtn))
		{
			if(showParamBtn.isSelected())
			{
				if(isSysOpt)
				{
					((SysOptPlaySheet)ps).hideAndClearSystemSelectPanel();//systemSelectPanel.setVisible(false);
					showSystemSelectBtn.setSelected(false);
					showSystemCapSelectBtn.setSelected(false);
				}
				ps.advParamPanel.setVisible(true);
			}
			else
				ps.advParamPanel.setVisible(false);
		}
		else if(isSysOpt)
		{
			ps.advParamPanel.setVisible(false);
			showParamBtn.setSelected(false);
			
			if(e.getSource().equals(showSystemSelectBtn))
			{
				showSystemCapSelectBtn.setSelected(false);
				((SysOptPlaySheet)ps).hideAndClearSystemSelectPanel();
				if(showSystemSelectBtn.isSelected())
				{
					((SysOptPlaySheet)ps).systemSelectPanel.setVisible(true);
					((SysOptPlaySheet)ps).capSelectDropDown.setVisible(false);
					((SysOptPlaySheet)ps).capScrollPanel.setVisible(false);
				}

			}
			else if(e.getSource().equals(showSystemCapSelectBtn))
			{
				showSystemSelectBtn.setSelected(false);
				((SysOptPlaySheet)ps).hideAndClearSystemSelectPanel();
				if(showSystemCapSelectBtn.isSelected())
				{
					((SysOptPlaySheet)ps).systemSelectPanel.setVisible(true);
					((SysOptPlaySheet)ps).capSelectDropDown.setVisible(true);
					((SysOptPlaySheet)ps).capScrollPanel.setVisible(true);
				}
			}	
		}

	}
	
	/**
	 * Determines the playsheet used for TAP service or TAP system optimization
	 * @param ps 	SerOptPlaySheet the playsheet used for TAP service or TAP system optimization
	 */
	public void setPlaySheet (SerOptPlaySheet ps)
	{
		this.ps = ps;
	}
	
	/**
	 * Determines the toggle buttons used to show panels for advanced parameters or system/capability selections
	 * @param showParamBtn 	JToggleButton the toggle button to show advanced parameters
	 */
	public void setParamButton (JToggleButton showParamBtn)
	{
		this.showParamBtn = showParamBtn;
		isSysOpt = false;
	}
	/**
	 * Determines the toggle buttons used to show panels for advanced parameters or system/capability selections
	 * @param showParamBtn 	JToggleButton the toggle button to show advanced parameters
	 * @param showSystemSelectBtn 	JToggleButton the toggle button to show system select panel
	 * @param showSystemCapSelectBtn 	JToggleButton the toggle button to show system and capability select panel
	 * 
	 */
	public void setParamButtons (JToggleButton showParamBtn, JToggleButton showSystemSelectBtn, JToggleButton showSystemCapSelectBtn)
	{
		this.showParamBtn = showParamBtn;
		this.showSystemSelectBtn = showSystemSelectBtn;
		this.showSystemCapSelectBtn = showSystemCapSelectBtn;
		isSysOpt = true;
	}
}