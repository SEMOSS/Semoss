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
 * This class is used to implement the breakeven function.
 */
public class BreakevenFunction {
	
	double[][] balanceList;
	ServiceOptimizer lin = null;
	/**
	 * Sets the specific service optimizer.
	 * @param lin 	Used to optimize calculations for TAP-specific services.
	 */
	public void setSvcOpt (ServiceOptimizer lin) 
	{
		this.lin=lin;
	}
	
	/**
	 * Finds the breakeven point assuming that the start year for implementation is 2014.
	 * @param totalYrs 	Total years service will be used.
	
	 * @return double	Breakeven point. */
	public double getZero(int totalYrs) {
		double thisYear = 2014;
		balanceList  = new double[totalYrs+1][2];
		balanceList[0][1]=0;
		balanceList[0][0]=thisYear;
		balanceList[1][1]=-lin.actualBudgetList.get(0);
		balanceList[1][0]=thisYear+1;
		for (int i=1; i<totalYrs;i++)
		{
			if (i<lin.actualBudgetList.size())
			{
				balanceList[i+1][1]=balanceList[i][1]-lin.actualBudgetList.get(i);
				for (int j=0; j<i;j++)
				{
					balanceList[i+1][1]=balanceList[i+1][1]+lin.objectiveValueList.get(j);
				}
			}
			else
			{
				balanceList[i+1][1]=balanceList[i][1];
				for (int j=0; j<lin.objectiveValueList.size();j++)
				{
					balanceList[i+1][1]=balanceList[i+1][1]+lin.objectiveValueList.get(j);
				}
			}
			balanceList[i+1][0]=thisYear+i+1;

		}
		double zero = findBreakEven(balanceList, thisYear);
		return zero;
	}
	
	/**
	 * Finds the breakeven point.
	 * @param graphPoints 	List of balances.
	 * @param startYear 	Start year of implementation.
	
	 * @return double		Breakeven point. */
	public double findBreakEven(double[][] graphPoints, double startYear)
	{
		double zero=0.0;
		for (int i=0; i<graphPoints.length-1;i++)
		{
			double x1=graphPoints[i][0];
			double y1=graphPoints[i][1];
			double x2=graphPoints[i+1][0];
			double y2=graphPoints[i+1][1];
			double slope = (y2-y1)/(x2-x1);
			zero = -y1/slope+x1;
			if (zero >i+startYear && zero <= i+startYear+1)
			{
				zero=zero-startYear;
				break;
			}
		}
		return zero;
	}
	
	
	
}
