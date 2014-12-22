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
package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;

import prerna.ui.components.specific.tap.OptimizationOrganizer;

/**
 * Interface representing a univariate real function that is implemented for TAP service optimization functions.
 */
public class UnivariateSvcOptFunction extends UnivariateOptFunction{

	public ServiceOptimizer lin;
	//double icdMt, serMain;
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
		lin = new ServiceOptimizer(icdMt,serMain);
		this.hourlyRate = hourlyRate;
		super.setVariables(numberOfYears, attRate, hireRate, infRate, disRate, secondProYear, initProc, secondProc);
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
	public void writeToAppConsole(ArrayList<Double> objList, ArrayList<Double> budgetList, double objFct)
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
	public String parseYearText(ArrayList<Double> list, String retString)
	{
		for (int i=0;i<list.size();i++)
		{
			int year = i+1;
			retString = retString+"Year "+ year+"-"+list.get(i)+", ";
		}
		return retString;
	}
}
