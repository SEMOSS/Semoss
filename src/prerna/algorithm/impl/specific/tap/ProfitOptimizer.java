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

import org.apache.commons.math3.exception.TooManyEvaluationsException;
import org.apache.commons.math3.optim.MaxEval;
import org.apache.commons.math3.optim.OptimizationData;
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType;
import org.apache.commons.math3.optim.univariate.BrentOptimizer;
import org.apache.commons.math3.optim.univariate.MultiStartUnivariateOptimizer;
import org.apache.commons.math3.optim.univariate.SearchInterval;
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction;
import org.apache.commons.math3.optim.univariate.UnivariateOptimizer;
import org.apache.commons.math3.optim.univariate.UnivariatePointValuePair;
import org.apache.commons.math3.random.RandomGenerator;
import org.apache.commons.math3.random.Well1024a;
import org.apache.log4j.Logger;

import prerna.ui.components.api.IPlaySheet;
import prerna.ui.components.specific.tap.SerOptPlaySheet;

/**
 * This class is used to optimize calculations for the profit function.
 */
public class ProfitOptimizer extends UnivariateSvcOptimizer{
	
	Logger logger = Logger.getLogger(getClass());

	/**
	 * This method runs the actual optimization to provide the optimized savings and the optimized budget constraint.
	 */
	public void optimize()
	{
        f = new ProfitFunction();
		super.optimize();
		Object[][] icdSerMatrix=(Object[][]) optOrg.icdService.get(optOrg.matrixLabel);
		if (icdSerMatrix.length==0)
		{
        	clearPlaysheet();
        	progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
        	return;
		}
	    double optSaving =0.0;
		
		UnivariateOptimizer optimizer = new BrentOptimizer(.001, .001);
        RandomGenerator rand = new Well1024a(500);
        MultiStartUnivariateOptimizer multiOpt = new MultiStartUnivariateOptimizer(optimizer, noOfPts, rand);
      
        UnivariateObjectiveFunction objF = new UnivariateObjectiveFunction(f);
        SearchInterval search = new SearchInterval(minBudget/hourlyCost,maxBudget/hourlyCost);
        optimizer.getStartValue();
        MaxEval eval = new MaxEval(200);
        
        OptimizationData[] data = new OptimizationData[]{search, objF, GoalType.MAXIMIZE, eval};
	       
        try {
            UnivariatePointValuePair pair = multiOpt.optimize(data);
            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
            playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nOptimized Savings: "+pair.getValue());
            playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nOptimized Budget Constraint: "+pair.getPoint()*hourlyCost);
            optSaving = pair.getValue();
            optBudget = pair.getPoint();
            //logger.debug( pair.getValue());
            //logger.debug( pair.getPoint());
        } catch (TooManyEvaluationsException fee) {
            // expected
        	playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nError: "+fee);
            progressBar.setIndeterminate(false);
            progressBar.setVisible(false);
        	clearPlaysheet();
        	//logger.debug(fee);
        }
        if (optSaving<=0)
        {
        	clearPlaysheet();
        }
        else
        {
        	runOptIteration();
        }
	}
	


/*
	private Hashtable createChart3()
	{
		Hashtable barChartHash = new Hashtable();
		Hashtable seriesHash = new Hashtable();
		Hashtable colorHash = new Hashtable();
		barChartHash.put("type",  "line");
		barChartHash.put("title",  "Cumulative Cost over Years");
		barChartHash.put("yAxisTitle", "Cumulative Cost");
		barChartHash.put("xAxisTitle", "Fiscal Year");
		String[] years = {"2014", "2015", "2016", "2017", "2018", "2019", "2020"};
		double[] icdCost = (double[]) inputValues.get("icdMainCost");
		double[] icdSOACost = (double[]) inputValues.get("soaICDMainCost");
		double[] soaCost = (double[]) inputValues.get("soaBuildCost");
		double[] totalCost = new double[7];
		double[] cumuICDCost = new double[7];
		cumuICDCost[0] = icdCost[0];
		double[] cumuSOACost = new double[7];

		for (int i=0; i<icdCost.length;i++)
		{
			totalCost[i]=icdSOACost[i]+soaCost[i];
		}
		cumuSOACost[0] = totalCost[0];
		for (int i= 1; i<cumuICDCost.length;i++)
		{
			cumuICDCost[i]=cumuICDCost[i-1]+icdCost[i];
			cumuSOACost[i]=cumuSOACost[i-1]+totalCost[i];
		}
		barChartHash.put("xAxis", years);
		seriesHash.put("Current As-Is ICD Maintenance Cost", cumuICDCost);
		seriesHash.put("Total Cost for Transition to SOA", cumuSOACost);
		colorHash.put("Current As-Is ICD Maintenance Cost", "#4572A7");
		colorHash.put("Total Cost for Transition to SOA", "#B5CA92");
		barChartHash.put("dataSeries",  seriesHash);
		barChartHash.put("colorSeries", colorHash);
		return barChartHash;
	}
*/
	/**
 * Casts a given playsheet as a service optimization playsheet.
 * @param playSheet 	Playsheet to be cast.
 */
@Override
	public void setPlaySheet(IPlaySheet playSheet) {
		this.playSheet = (SerOptPlaySheet) playSheet;
		
	}

	/**
	 * Gets variable names.
	// TODO: Don't return null
	 * @return String[] */
	@Override
	public String[] getVariables() {
		
		return null;
	}

	/**
	 * Executes the optimization.
	 */
	@Override
	public void execute(){
		optimize();
		
	}

	/**
	 * Gets algorithm name.
	// TODO: Don't return null
	 * @return String */
	@Override
	public String getAlgoName() {
		
		return null;
	}

}
