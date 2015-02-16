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
package prerna.algorithm.impl.specific.tap;

import lpsolve.LpSolve;
import lpsolve.LpSolveException;

public class ResidualSystemTheatGarrOptimizer extends ResidualSystemOptimizer{
	
	int[] systemTheater;
	int[] systemGarrison;
	
	//Ap, Bq
	int[][] dataRegionSORSystemTheaterExists;
	int[][] dataRegionSORSystemGarrisonExists;
	int[][] bluRegionProviderTheaterExists;
	int[][] bluRegionProviderGarrisonExists;

	/**
	 * Gathers data set.
	 */
	public void setTheatGarrDataSet(int[] systemTheater,int[] systemGarrison,int[][] dataRegionSORSystemTheaterExists, int[][] dataRegionSORSystemGarrisonExists, int[][] bluRegionProviderTheaterExists, int[][] bluRegionProviderGarrisonExists) {
		this.systemTheater = systemTheater;
		this.systemGarrison = systemGarrison;
		this.dataRegionSORSystemTheaterExists=dataRegionSORSystemTheaterExists;
		this.dataRegionSORSystemGarrisonExists=dataRegionSORSystemGarrisonExists;
		this.bluRegionProviderTheaterExists=bluRegionProviderTheaterExists;
		this.bluRegionProviderGarrisonExists=bluRegionProviderGarrisonExists;
	}
	
	/**
	 * Sets constraints in the model.
	 */
	@Override
	public void setConstraints() {
		//makes building the model faster if it is done rows by row
		solver.setAddRowmode(true);	
		//adding constraints for data objects
		addRequiredSystemsConstraint(systemModernize);
		addDecommissionSystemsConstraint(systemDecommission);
		if(systemTheater!=null) {
			addConstraints(systemDataMatrix,systemRegionMatrix,systemTheater,dataRegionSORSystemTheaterExists);
			addConstraints(systemBLUMatrix,systemRegionMatrix,systemTheater,bluRegionProviderTheaterExists);
		}
		if(systemGarrison!=null) {
			addConstraints(systemDataMatrix,systemRegionMatrix,systemGarrison,dataRegionSORSystemGarrisonExists);
			addConstraints(systemBLUMatrix,systemRegionMatrix,systemGarrison,bluRegionProviderGarrisonExists);
		}
		//rowmode turned off
		solver.setAddRowmode(false);
	}
	
	private void addConstraints(int[][] systemProviderMatrix, int[][] systemRegionMatrix, int[] systemGT, int[][] constraintMatrix)
	{
		try{
			for(int dataInd=0;dataInd<systemProviderMatrix[0].length;dataInd++)
			{
				for(int regionInd=0;regionInd<systemRegionMatrix[0].length;regionInd++)
				{
					int[] colno = new int[systemProviderMatrix.length];
			        double[] row = new double[systemProviderMatrix.length];
		
			        for(int sysInd=0;sysInd<systemProviderMatrix.length;sysInd++)
			        {
			        	colno[sysInd] = sysInd+1;
			        	row[sysInd] = systemProviderMatrix[sysInd][dataInd]*systemRegionMatrix[sysInd][regionInd]*systemGT[sysInd];
			        }
			        if(constraintMatrix[dataInd][regionInd]>0)
			        	solver.addConstraintex(systemProviderMatrix.length, row, colno, LpSolve.GE, 1);
			        else
			        	solver.addConstraintex(systemProviderMatrix.length, row, colno, LpSolve.GE, 0);
				}
			}
		}catch (LpSolveException e){
			e.printStackTrace();
		}
	}
	
}
