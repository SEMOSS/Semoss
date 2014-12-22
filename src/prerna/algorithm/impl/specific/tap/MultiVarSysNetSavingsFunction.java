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

/**
 * This class is used to calculate the number of years to decommission systems based on the budget.
 * It is used in TAP Systems optimization.
 */
public class MultiVarSysNetSavingsFunction extends MultivariateOptFunction{
	
	/**
	 * Given a specific budget, calculates the savings up until .
	 * Gets the list of potential yearly savings and yearly budgets.
	 * @param a 	Budget used in the service optimizer.
	 * @return 		Profit. */
	@Override
	public double calculateRet(double[] budget, double n)
	{
		calculateInvestment(budget,n);
		double savings = calculateSavingsForVariableTotal(n,totalYrs);
		savings = savings - investment;
		return savings;
	}
	
	@Override	
	public void writeToAppConsole(double[] budget, double n, double savings)
	{
		consoleArea.setText(consoleArea.getText()+"\nPerforming optimization iteration "+count);
		consoleArea.setText(consoleArea.getText()+"\nFor Budget B: ");
		for(int i=0;i<budget.length;i++)
			consoleArea.setText(consoleArea.getText()+budget[i]+", ");
		consoleArea.setText(consoleArea.getText()+"the minimum N is "+n+" and the savings are "+savings);
	}
}
