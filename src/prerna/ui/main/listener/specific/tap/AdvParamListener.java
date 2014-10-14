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
	JToggleButton showParamBtn, showSystemSelectBtn, showSystemCapSelectBtn, showSystemModDecomBtn;
	boolean isSysOpt;
	
	/**
	 * Action for when showParamBtn, showSystemSelectBtn, or showSystemCapSelectBtn is pressed
	 * on the TAP service or TAP system optimization playsheet
	 * When pressed, makes advParamPanel become visible or system/capability panel visible
	 * @param e ActionEvent
	 */
	public void actionPerformed(ActionEvent e) {
		if(((JToggleButton)e.getSource()).getName().equals(showParamBtn.getName()))
		{
			if(showParamBtn.isSelected()) {
				if(isSysOpt) {
					showSystemSelectBtn.setSelected(false);
					showSystemCapSelectBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					((SysOptPlaySheet)ps).systemDataBLUSelectPanel.setVisible(false);
					((SysOptPlaySheet)ps).systemModDecomSelectPanel.setVisible(false);
				}
				ps.advParamPanel.setVisible(true);
			}
			else
				ps.advParamPanel.setVisible(false);
		}
		else if(isSysOpt)
		{
			if(((JToggleButton)e.getSource()).getName().equals(showSystemSelectBtn.getName()))
			{
				if(showSystemSelectBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemCapSelectBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					ps.advParamPanel.setVisible(false);
					((SysOptPlaySheet)ps).systemModDecomSelectPanel.setVisible(false);
					((SysOptPlaySheet)ps).systemDataBLUSelectPanel.setVisible(true);
					((SysOptPlaySheet)ps).capabilitySelectPanel.setVisible(false);
					((SysOptPlaySheet)ps).capabilitySelectPanel.clearList();
					//if data or BLU was selected, show it. otherwise hide panel
					if(((SysOptPlaySheet)ps).dataBLUSelectPanel.noneSelected()) {
						((SysOptPlaySheet)ps).updateDataBLUPanelButton.setSelected(false);
						((SysOptPlaySheet)ps).dataBLUSelectPanel.setVisible(false);
					} else {
						((SysOptPlaySheet)ps).updateDataBLUPanelButton.setSelected(true);
						((SysOptPlaySheet)ps).dataBLUSelectPanel.setVisible(true);
						((SysOptPlaySheet)ps).dataBLUSelectPanel.setFromSystem(true);
					}
						
				} else {
					((SysOptPlaySheet)ps).systemDataBLUSelectPanel.setVisible(false);
				}
			}
			else if(((JToggleButton)e.getSource()).getName().equals(showSystemCapSelectBtn.getName()))
			{
				if(showSystemCapSelectBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemSelectBtn.setSelected(false);
					showSystemModDecomBtn.setSelected(false);
					ps.advParamPanel.setVisible(false);
					((SysOptPlaySheet)ps).systemModDecomSelectPanel.setVisible(false);
					((SysOptPlaySheet)ps).systemDataBLUSelectPanel.setVisible(true);
					((SysOptPlaySheet)ps).capabilitySelectPanel.setVisible(true);
					//if data or BLU was selected, show it. otherwise hide panel
					if(((SysOptPlaySheet)ps).dataBLUSelectPanel.noneSelected()) {
						((SysOptPlaySheet)ps).updateDataBLUPanelButton.setSelected(false);
						((SysOptPlaySheet)ps).dataBLUSelectPanel.setVisible(false);
					} else {
						((SysOptPlaySheet)ps).updateDataBLUPanelButton.setSelected(true);
						((SysOptPlaySheet)ps).dataBLUSelectPanel.setVisible(true);
						((SysOptPlaySheet)ps).dataBLUSelectPanel.setFromSystem(false);					}
				} else {
					((SysOptPlaySheet)ps).systemDataBLUSelectPanel.setVisible(false);
				}
			}
			else if(((JToggleButton)e.getSource()).getName().equals(showSystemModDecomBtn.getName()))
			{
				if(showSystemModDecomBtn.isSelected()) {
					showParamBtn.setSelected(false);
					showSystemSelectBtn.setSelected(false);
					showSystemCapSelectBtn.setSelected(false);
					ps.advParamPanel.setVisible(false);
					((SysOptPlaySheet)ps).systemDataBLUSelectPanel.setVisible(false);
					((SysOptPlaySheet)ps).systemModDecomSelectPanel.setVisible(true);
				} else {
					((SysOptPlaySheet)ps).systemModDecomSelectPanel.setVisible(false);
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
	public void setParamButtons (JToggleButton showParamBtn, JToggleButton showSystemSelectBtn, JToggleButton showSystemCapSelectBtn, JToggleButton showSystemModDecomBtn)
	{
		this.showParamBtn = showParamBtn;
		this.showSystemSelectBtn = showSystemSelectBtn;
		this.showSystemCapSelectBtn = showSystemCapSelectBtn;
		this.showSystemModDecomBtn = showSystemModDecomBtn;
		isSysOpt = true;
	}
}