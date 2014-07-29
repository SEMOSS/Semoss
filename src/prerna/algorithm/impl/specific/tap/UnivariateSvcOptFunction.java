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

import java.util.ArrayList;
import java.util.Hashtable;

import javax.swing.JProgressBar;
import javax.swing.JTextArea;

import org.apache.commons.math3.analysis.UnivariateFunction;

import prerna.ui.components.specific.tap.OptimizationOrganizer;

/**
 * Interface representing a univariate real function that is implemented for TAP service optimization functions.
 */
public class UnivariateSvcOptFunction extends UnivariateOptFunction{

	public ServiceOptimizer lin;
	double icdMt, serMain;
	double hourlyRate;
	
	/**
	 * Sets variables used in the optimization. 
	 * @param 	numberOfYears		Total number of years service is used.
	 * @param 	icdMt				ICD maintenance costs over a single year. 
	 * @param 	serMain				% of SDLC total required to maintain service.
	 * @param 	attRate				Attrition rate (how many employees leave) over a year.
	 * @param 	hireRate			Hire rate over the year.
	 * @param 	infRate				Inflation rate over the year.
	 * @param 	disRate				Discount rate over the year.
	 * @param 	secondProYear		Second pro year - year in which more information is known.
	 * @param 	initProc			How much information you have initially.
	 * @param 	secondProc			How much information you have at second pro year.
	 */
	public void setVariables(int numberOfYears, double hourlyRate, double icdMt, double serMain, double attRate, double hireRate, double infRate, double disRate, int secondProYear, double initProc, double secondProc){
		super.setVariables(numberOfYears, attRate, hireRate, infRate, disRate, secondProYear, initProc, secondProc);
		lin = new ServiceOptimizer(icdMt,serMain);
		this.hourlyRate = hourlyRate;
	}
	
	/**
	 * Sets data in the service optimizer.
	 * @param optOrg 	Optimization Organizer is used to efficiently run TAP-specific optimizations.
	 */
	public void setData(OptimizationOrganizer optOrg){
		lin.setData(optOrg);
	}
	
	/**
	 * 	Writes information to a JTextArea console.
	 * @param	objList		List of potential yearly savings for current iteration.
	 * @param 	budgetList	List of yearly budgets for current iteration.
	 * @param 	objFct		Objective function used in optimization.
	 */
	public void writeToAppConsole(ArrayList objList, ArrayList budgetList, double objFct)
	{
		consoleArea.setText(consoleArea.getText()+"\nPerforming yearly linear optimization iteration "+count);
		String budgetText = parseYearText(budgetList,"Year-by-year Budgets for current iteration:");
		String objText = parseYearText(objList,"Year-by-year savings potential for current iteration:");
		consoleArea.setText(consoleArea.getText()+"\n"+budgetText);
		consoleArea.setText(consoleArea.getText()+"\n"+objText);
		consoleArea.setText(consoleArea.getText()+"\nTotal obj function: "+objFct);
	}
	/**
	 * Loops through a list and parses through each element to return the year text.
	 * @param 	list		List.
	 * @param 	retString	String to be returned.
	
	 * @return 	retString	Year text. */
	public String parseYearText(ArrayList list, String retString)
	{
		for (int i=0;i<list.size();i++)
		{
			int year = i+1;
			retString = retString+"Year "+ year+"-"+list.get(i)+", ";
		}
		return retString;
	}
}
