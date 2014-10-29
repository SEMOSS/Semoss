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
package prerna.ui.components.specific.tap;

import javax.swing.JButton;

import prerna.ui.main.listener.specific.tap.MultiVarSysOptBtnListener;

/**
 * This is the playsheet used exclusively for TAP system optimization with annually varying budget.
 */
@SuppressWarnings("serial")
public class MultiVarSysOptPlaySheet extends SysOptPlaySheet{

	/**
	 * Constructor for MultiVarSysOptPlaySheet.
	 */
	public MultiVarSysOptPlaySheet() {
		super();
	}

	@Override
	public void addOptimizationBtnListener(JButton btnRunOptimization) {
		MultiVarSysOptBtnListener obl = new MultiVarSysOptBtnListener();
		obl.setOptPlaySheet(this);
		btnRunOptimization.addActionListener(obl);
	}
}
