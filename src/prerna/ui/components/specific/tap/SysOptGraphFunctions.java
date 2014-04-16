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

import java.util.ArrayList;
import java.util.Hashtable;

import prerna.algorithm.impl.specific.tap.SysNetSavingsOptimizer;

/**
 * This class is used to optimize graph functions used in services calculations.
 */
public class SysOptGraphFunctions extends SerOptGraphFunctions{

	
	@Override
	public ArrayList<double[]> createSavingsPerYearList()
	{
		ArrayList<double[]> savingsPerYearList = new ArrayList<double[]>();
		for (int i=0 ;i< 1;i++)
		{
			double[] newYear = new double[opt.maxYears];
			for (int j=i;j<opt.maxYears;j++)
			{
				newYear[j]=((SysNetSavingsOptimizer)opt).cumSavingsList.get(j);
			}
			savingsPerYearList.add(newYear);
		}
		return savingsPerYearList;
	}
	
	@Override
	public double[][] createBalanceList(int thisYear)
	{
		thisYear = 2014;
		double[][] balanceList  = new double[opt.maxYears][2];
		for (int i=0; i<opt.maxYears;i++)
		{	
			balanceList[i][0]=thisYear+i;
			balanceList[i][1]=((SysNetSavingsOptimizer)opt).breakEvenList.get(i);
		}
		return balanceList;
	}
	

}
