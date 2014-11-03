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

import prerna.algorithm.impl.LinearInterpolation;


/**
 * SysIRRLinInterp is used to estimate the IRR (mu) for a given discount rate using Linear Interpolation.
 */
public class MultiVarSysIRRLinInterp extends LinearInterpolation{

	double numMaintenanceSavings, serMainPerc, dataExposeCost, totalYrs, infRate,discRate;
	double N;
	double[] B;
	String printString = "";

	/**
	 * Sets the parameters used in the equation.
	 * @param numMaintenanceSavings	
	 * @param serMainPerc
	 * @param dataExposeCost
	 * @param totalYrs
	 * @param infRate
	 * @param discRate
	 */
	public void setCalcParams(double numMaintenanceSavings,double serMainPerc,double dataExposeCost,double totalYrs,double infRate,double discRate) {
		this.numMaintenanceSavings=numMaintenanceSavings;
		this.serMainPerc=serMainPerc;
		this.dataExposeCost = dataExposeCost;
		this.totalYrs=totalYrs;
		this.infRate=infRate;
		this.discRate=discRate;
	}
	/**
	 * Sets the Budget and Number of years given
	 * @param B	Double	Budget
	 * @param N Double	Number of years to transition.
	 */
	public void setBAndN(double[] B,double N) {
		this.B = B;
		this.N = N;
	}
	
	/**
	 * Gets the calculation string that details the calculation performed for debugging purposes.
	 * @return printString
	 */
	public String getPrintString() {
		return printString;
	}
	

	/**
	 * Calculate the residual value for a given root estimate, possibleDiscRate.
	 * @param possibleDiscRate double	root estimate	
	 * @return Double	residual value for the calculation
	 */
	@Override
	public Double calcY(double possibleDiscRate) {		
		double v = (1+infRate)/(1+possibleDiscRate);
		double vFactor = totalYrs-N;
		if(v!=1)
			vFactor = Math.pow(v, N+1.0)*(1.0-Math.pow(v, totalYrs-N))/(1.0-v);
		double sustainSavings = vFactor*(numMaintenanceSavings - serMainPerc*dataExposeCost);
		double mu = (1+infRate)/(1+discRate);
		double investment = 0.0;
		for(int q=0;q<B.length;q++) {
			double P1Inflation = 1.0;
			if(mu!=1)
				P1Inflation = Math.pow(mu, q);
			investment += B[q]* P1Inflation;
		}
		double yVal = sustainSavings - investment;
		printString += "\nv: "+v+" v^(N+1)*(1-v^(Q-N))/(1-v) "+vFactor+"\nmu: "+mu+" investment "+investment;
		return yVal;
	}
}
