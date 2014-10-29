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
package prerna.algorithm.impl.specific.tap;


/**
 * This optimizer is used for implementation of the ROI (return on investment) function.
 */
public class MultiVarSysROIOptimizer extends MultivariateOptimizer{
	
	/**
	 * Runs the appropriate optimization iteration.
	 */
	@Override
	public void optimize()
	{
        f = new MultiVarSysROIFunction();
        super.optimize();
        if(noErrors)
        {
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nBudget: "+yearlyBudget);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nNumber of Years to consolidate systems: "+optNumYears);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nGiven timespan to accumulate savings over: "+maxYears);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nMaximized ROI: "+roi);
        }
	}   
	        
}
