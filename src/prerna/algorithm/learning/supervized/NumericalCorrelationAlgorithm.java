package prerna.algorithm.learning.supervized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticActionRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.om.SEMOSSParam;
import prerna.ui.components.playsheets.MatrixRegressionHelper;

public class NumericalCorrelationAlgorithm implements IAnalyticActionRoutine {

	private static final Logger LOGGER = LogManager.getLogger(NumericalCorrelationAlgorithm.class.getName());

	public static final String INCLUDE_INSTANCES = "includeInstances";
	public static final String SKIP_ATTRIBUTES = "skipAttributes";

	private ITableDataFrame dataFrame;
	private double[][] correlationArray;
	private boolean includesInstance = false;
	private List<String> skipAttributes;
	private ArrayList<SEMOSSParam> options;

	/**
	 * Calculates the numerical correlation between all the variables in a table
	 */
	public NumericalCorrelationAlgorithm() {
		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(INCLUDE_INSTANCES);
		options.add(0, p1);
		
		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(SKIP_ATTRIBUTES);
		options.add(1, p2);
	}

	@Override
	public void runAlgorithm(ITableDataFrame... data) {
		this.dataFrame = data[0];
		this.includesInstance = (boolean) options.get(0).getSelected();
		this.skipAttributes = (List<String>) options.get(1).getSelected();
		
		dataFrame.setColumnsToSkip(skipAttributes);
		int numCols = dataFrame.getColumnHeaders().length;

		//create the b and A arrays which are used in matrix regression to determine coefficients
		double[][] dataArr;
		if(this.includesInstance) {
			dataArr = MatrixRegressionHelper.createA(this.dataFrame, 1, numCols);
		} else {
			dataArr = MatrixRegressionHelper.createA(this.dataFrame, 0, numCols);
		}
		
		PearsonsCorrelation correlation = new PearsonsCorrelation(dataArr);
		correlationArray = correlation.getCorrelationMatrix().getData();	
	}
	
	@Override
	public Object getAlgorithmOutput() {
		int numVariables;
		String id;
		String[] columnHeaders = this.dataFrame.getColumnHeaders();
		if(includesInstance) {
			numVariables = columnHeaders.length - 1;
			id = columnHeaders[0];
		}else {
			numVariables = columnHeaders.length;
			id = "";
		}
		
		// reversing values since it is being painted by JS in reverse order
		double[][] correlations = new double[numVariables][numVariables];
		int i = 0;
		int j = 0;
		for(i = 0; i<numVariables; i++) {
			for(j = 0; j<numVariables; j++) {
				correlations[numVariables-i-1][numVariables-j-1] = correlationArray[i][j];
			}
		}
		
		Map<String, String> dataTableAlign = new HashMap<String, String>();
		for(i = 0; i < columnHeaders.length; i++) {
			dataTableAlign.put("dim " + i,columnHeaders[i]);
		}

		Hashtable<String, Object> dataHash = new Hashtable<String, Object>();
		dataHash.put("one-row",false);
		dataHash.put("id",id);
		dataHash.put("correlations", correlations);

		Hashtable<String, Object> allHash = new Hashtable<String, Object>();
		allHash.put("specificData", dataHash);
		allHash.put("data", this.dataFrame.getData());
		allHash.put("headers", columnHeaders);
		allHash.put("layout", getDefaultViz());
		allHash.put("dataTableAlign", dataTableAlign);

		return allHash;
	}

	@Override
	public String getName() {
		return "Matrix Regression";
	}

	@Override
	public String getResultDescription() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet) {
			for(SEMOSSParam param : options) {
				if(param.getName().equals(key)){
					param.setSelected(selected.get(key));
					break;
				}
			}
		}
	}
		
	@Override
	public List<SEMOSSParam> getOptions() {
		return this.options;
	}

	@Override
	public String getDefaultViz() {
		return "ScatterplotMatrix";
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public double[][] getCorrelationArray() {
		return correlationArray;
	}
}