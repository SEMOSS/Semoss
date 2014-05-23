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
package prerna.algorithm.impl;

import prerna.algorithm.api.IAlgorithm;
import prerna.ui.components.api.IPlaySheet;


/**
 * This class is used to estimate y-intercept (root) of equation for a nonlinear function.
 */
public class LinearInterpolation implements IAlgorithm{

	IPlaySheet playSheet;
	final double epsilon = 0.00001;
	double min, max;
	double numMaintenanceSavings, serMainPerc, dataExposeCost, totalYrs, infRate,discRate;
	double N, B;
	double a, b, m, y_m, y_a, y_b;
	
	public double retVal = -1.0;
    
	
	/**
	 * Executes the process of estimating a root.
	 * Calculates the y values at min and max x's.
	 * Finds the midpoint and determines what the y value is for this point.
	 * Sets either the min/max y values to be the midpoint depending on which way it needs to go.
	 * 
	 * a and b are the bounds for the value being solved for. In this case, solving for mu.
	 * y_a and y_b are the years
	 */
	public void execute(){

		retVal = -1.0E30;
	   	a = min;
	    b = max;
	   	
	   		y_b = calcY(b);
	   	    y_m = calcY(m);			// y_m = f(m)
	   	    y_a = calcY(a);			// y_a = f(a)
	   	 if(a<0&&b<0 ||(y_b > 0 && y_a > 0) || (y_b < 0 && y_a < 0))
	   		 return;
	   	 while ( (b-a) > epsilon )
	   	 {
	   	    m = (a+b)/2;           // Mid point
	    
	   	    y_m = calcY(m);			// y_m = f(m)
	   	    y_a = calcY(a);			// y_a = f(a)
	    
	   	    if ( (y_m > 0 && y_a < 0) || (y_m < 0 && y_a > 0) )
	   	    {  // f(a) and f(m) have different signs: move b
	   	       b = m;
	   	    }
	   	    else
	   	    {  // f(a) and f(m) have same signs: move a
	   	       a = m;
	   	    }
	                                           // Print progress  
	   	 }
	    
	   	 retVal = (a+b)/2;
	   	 if((max-retVal)<.001)
	   		 retVal =  -1.0E30;
	}
	
	public void setValues(double numMaintenanceSavings,double serMainPerc,double dataExposeCost,double totalYrs,double infRate,double discRate,double min, double max)
	{
		this.numMaintenanceSavings=numMaintenanceSavings;
		this.serMainPerc=serMainPerc;
		this.dataExposeCost = dataExposeCost;
		this.totalYrs=totalYrs;
		this.infRate=infRate;
		this.discRate=discRate;
		this.min = min;
		this.max = max;
	}
	public void setBAndN(double B,double N)
	{
		this.B = B;
		this.N = N;
	}
	
	public double calculateInvestment(double mu)
	{
		double investment = 0.0;
		double P1InflationSum = 0.0;
		for(int q=1;q<=N;q++)
		{
			double P1Inflation = 1.0;
			if(mu!=1)
				P1Inflation = Math.pow(mu, q-1);
			P1InflationSum += P1Inflation;
		}
		double extraYear = 1.0;
		if(mu!=1)
			extraYear = Math.pow(mu,Math.ceil(N));
		P1InflationSum+=extraYear*(N-Math.floor(N));
		investment = B * P1InflationSum;
		return investment;
	}
	
	//equation that we are trying to make equal to 0.
	public Double calcY(double possibleDiscRate)
	{		
		double mu = (1+infRate)/(1+possibleDiscRate);
		double muFactor = totalYrs-N;
		if(mu!=1)
			muFactor = Math.pow(mu, N+1)*(1-Math.pow(mu, totalYrs-N))/(1-mu);
		double sustainSavings = muFactor*(numMaintenanceSavings - serMainPerc*dataExposeCost);
		double muWithDiscount = (1+infRate)/(1+discRate);
		double investment = calculateInvestment(muWithDiscount);
		double yVal = sustainSavings - investment;
		return yVal;
	}


	/**
	 * Sets playsheet as a graph playsheet.
	 * @param ps IPlaySheet		Playsheet to be cast.
	 */
	public void setPlaySheet(IPlaySheet ps){
		playSheet = ps;
	}

	/**
	 * Gets variable names.
	
	 * //TODO: Return empty object instead of null
	 * @return String[] 	List of variable names in a string array. */
	@Override
	public String[] getVariables() {
		return null;
	}

	/**
	 * Gets algorithm name - in this case, "Loop Identifier."
	
	 * @return String		Name of algorithm. */
	@Override
	public String getAlgoName() {
		return "Linear Interpolation";
	}

}
