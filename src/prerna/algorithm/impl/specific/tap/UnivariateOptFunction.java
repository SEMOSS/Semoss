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

import javax.swing.JProgressBar;
import javax.swing.JTextArea;

import org.apache.commons.math3.analysis.UnivariateFunction;

/**
 * Interface representing a univariate real function that is implemented for TAP system and service optimization functions.
 */
public class UnivariateOptFunction implements UnivariateFunction{
	
	public int totalYrs;
	double cnstC, cnstK;
	public double attRate, hireRate, infRate, disRate;
	
	public double[] learningConstants;
	int count = 0;
	
	JTextArea consoleArea;
	boolean write = true;
	JProgressBar progressBar;
	
	/**
	 * Sets variables used in the optimization. 
	 * @param 	numberOfYears		Total number of years service is used.
	 * @param 	attRate				Attrition rate (how many employees leave) over a year.
	 * @param 	hireRate			Hire rate over the year.
	 * @param 	infRate				Inflation rate over the year.
	 * @param 	disRate				Discount rate over the year.
	 * @param 	secondProYear		Second pro year - year in which more information is known.
	 * @param 	initProc			How much information you have initially.
	 * @param 	secondProc			How much information you have at second pro year.
	 */
	public void setVariables(int numberOfYears, double attRate, double hireRate, double infRate, double disRate, int secondProYear, double initProc, double secondProc){
		this.attRate = attRate;
		this.hireRate = hireRate;
		this.infRate = infRate;
		this.disRate = disRate;
		totalYrs = numberOfYears;
		
		createLearningYearlyConstants(numberOfYears, secondProYear, initProc, secondProc);
	}
	
	
	/**
	 * value
	 * @param arg0 double
	// TODO: Return empty object instead of null
	 * @return double */
	public double value(double arg0) {
		return 0;
	}
	
	/**
	 * Sets the console area.
	 * @param JTextArea		Console area.
	 */
	public void setConsoleArea (JTextArea consoleArea)
	{
		this.consoleArea=consoleArea;
	}

	/**
	 * Sets properties of the progress bar.
	 * @param bar	Original bar that updates are made to.
	 */
	public void setProgressBar (JProgressBar bar)
	{
		this.progressBar=bar;
		this.progressBar.setVisible(true);
		this.progressBar.setIndeterminate(true);
		this.progressBar.setStringPainted(true);
	}
	
	/**
	 * Sets the value of the progress string on the progress bar.
	 * @param text	Text to be set.
	 */
	public void updateProgressBar (String text)
	{

		this.progressBar.setString(text);
	}
	
	/**
	 * Sets the write boolean.
	 * @param write	Boolean that is either true or false depending on optimization.
	 */
	public void setWriteBoolean (boolean write)
	{
		this.write = write;
	}
	
	
	/**
	 * Solve differential equations in order to obtain the learning curve yearly constants for a service.
	 * @param 	numberOfYears	Number of years service is used.
	 * @param 	secondProYear	Second pro year - year in which more information is known.
	 * @param 	initProC		How much information you have initially.
	 * @param 	secondProC		How much information you have at second pro year.
	
	 * @return 	An array of doubles containing the learning curve constants. */
	public double[] createLearningYearlyConstants(int numberOfYears, int secondProYear, double initProC, double secondProC)
	{
		//learning curve differential equation f(t) = Pmax-C*e^(-k*t)
		//after you solve for differential equation to get constants
		//here are the equations for the constants
		
		cnstC = 1.0-initProC;
		cnstK = (1.0/secondProYear)*Math.log((1.0-initProC)/(1.0-secondProC));
		int yearToCalc = Math.max(totalYrs, 10);
		learningConstants = new double[yearToCalc];
		double[] origLearningConstants = new double[yearToCalc];
		for (int i = 0; i<origLearningConstants.length;i++)
		{
			origLearningConstants[i]=1.0+(cnstC/cnstK)*Math.exp(-(i+1.0)*cnstK)*(1.0-Math.exp(cnstK));
		}
		for (int i = 0; i<learningConstants.length;i++)
		{
			//account for turnover
			//ensure number of iterations does not pass
			for (int j=0;j<i;j++)
			{
				learningConstants[i]=learningConstants[i]+origLearningConstants[j]*hireRate*Math.pow(1-attRate, j);
			}
			learningConstants[i]=learningConstants[i]+origLearningConstants[i]*Math.pow((1-attRate),i);
		}
		return learningConstants;
		
	}
}
