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
	double alpha0, k, sigma, dataExposeCost, B, min, max;
	double a, b, m, y_m, y_a;
	
	public double retVal = -1.0;
    
	
	/**
	 * Executes the process of estimating a root.
	 * Calculates the y values at min and max x's.
	 * Finds the midpoint and determines what the y value is for this point.
	 * Sets either the min/max y values to be the midpoint depending on which way it needs to go.
	 */
	public void execute(){

	   	 a = min;
	   	 b = max;
	   	 
	   	 if(a<0&&b<0)
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
	}
	
	public void setValues(double alpha0,double alpha, double r,double dataExposeCost, double min, double max)
	{
		this.alpha0 = alpha0;
		this.k = (1/r)*Math.log(((1-alpha0)/(1-alpha)));
		this.sigma = ((1-alpha0)/k)*(1-Math.exp(k));
		this.dataExposeCost = dataExposeCost;
		this.min = min;
		this.max = max;
	}
	public void setB(double B)
	{
		this.B = B;
	}
	
	public Double calcY(double x)
	{		
		double yVal = 0.0;
		yVal = x;
		yVal += sigma * (1 - Math.exp(-1*(x+1)*k))/(1-Math.exp(-1*k));
		yVal -= dataExposeCost / B;
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
