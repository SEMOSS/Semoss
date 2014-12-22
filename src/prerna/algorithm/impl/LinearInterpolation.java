/*******************************************************************************
 * Copyright 2014 SEMOSS.ORG
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *******************************************************************************/
package prerna.algorithm.impl;

import prerna.algorithm.api.IAlgorithm;
import prerna.ui.components.api.IPlaySheet;


/**
 * LinearInterpolation is used to estimate the y-intercept (root) of equation for a nonlinear function.
 */
public class LinearInterpolation implements IAlgorithm{

	IPlaySheet playSheet;
	final double epsilon = 0.00001;
	double min, max;
	double a, b, m, y_m, y_a, y_b;
	
	public double retVal = -1.0;    
	
	/**
	 * Executes the process of estimating a root.
	 * Given a min and max value for the root (the x values).
	 * Calculates the y values at min and max x's.
	 * Finds the midpoint and determines what the y value is for this point.
	 * Adjusts either the min/max x value depending on which segment contains the midpoint.
	 * If min and midpoint have opposite signs, root is in between, so max x value becomes midpoint.
	 * 
	 * Stores the y-intercept (root) of the function, or -1.0E30 if nothing is found in retVal.

	 */
	public void execute(){

	   	a = min;
	    b = max;
	   	
	   		y_b = calcY(b);
	   	    y_m = calcY(m);			// y_m = f(m)
	   	    y_a = calcY(a);			// y_a = f(a)
	   	 if(a<0&&b<0 ||(y_b > 0 && y_a > 0) || (y_b < 0 && y_a < 0)) {
	   		 retVal = -1.0E30;
	   		 return;
	   	 }
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
	   	 }
	   	 retVal = (a+b)/2;
	   	 if((max-retVal)<.001)
	   		 retVal =  -1.0E30;
	}
	/**
	 * Sets the min and max values for the root estimate.
	 * @param min	minimum possible value for the root
	 * @param max	maximum possible value for the root
	 */
	public void setMinAndMax(double min, double max)
	{
		this.min = min;
		this.max = max;
	}
	/**
	 * Calculate the residual value for a given root estimate, xVal.
	 * @param xVal double	root estimate	
	 * @return Double	residual value for the calculation
	 */
	public Double calcY(double xVal)
	{		
		return -1.0;
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
	 * @return String[] 	List of variable names in a string array. */
	@Override
	public String[] getVariables() {
		String[] variables = new String[1];
		return variables;
	}

	/**
	 * Gets algorithm name - in this case, "Loop Identifier."
	
	 * @return String		Name of algorithm. */
	@Override
	public String getAlgoName() {
		return "Linear Interpolation";
	}

}
