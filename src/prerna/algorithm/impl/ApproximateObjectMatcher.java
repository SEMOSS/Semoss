package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.nlp.NaturalLanguageProcessingHelper;
import prerna.ds.BTreeDataFrame;
import prerna.math.StatisticsUtilityMethods;
import prerna.om.SEMOSSParam;
import prerna.util.Constants;
import prerna.util.DIHelper;
import rita.RiTa;
import rita.RiWordNet;
import rita.wordnet.RiWordNetError;

public class ApproximateObjectMatcher implements IAnalyticRoutine {

	private static final Logger LOGGER = LogManager.getLogger(ExactStringMatcher.class.getName());
	private List<SEMOSSParam>  options;
	public final String COLUMN_ONE_KEY = "table1Col";
	public final String COLUMN_TWO_KEY = "table2Col";
	private Map<String, Object> resultMetadata = new HashMap<String, Object>();

	private RiWordNet wordnet;
	private boolean useDefaultThreshold = false;
	private double threshold = 0.1;
	
//	private boolean includeNonMatched = false;
	
	public ApproximateObjectMatcher(){
		this.options = new ArrayList<SEMOSSParam>();
		
		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName(this.COLUMN_ONE_KEY);
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName(this.COLUMN_TWO_KEY);
		options.add(1, p2);
	}
	
	@Override
	public void setSelectedOptions(Map<String, Object> selected) {
		Set<String> keySet = selected.keySet();
		for(String key : keySet)
		{
			for(SEMOSSParam param : options)
			{
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
	
//	public void setIncludeNonMatched(boolean includeNonMatched) {
//		this.includeNonMatched = includeNonMatched;
//	}
//	
	public void setThreshold(double threshold) {
		this.threshold = threshold;
		this.useDefaultThreshold = true;
	}
	
	public void setUseDefaultThreshold(boolean useDefaultThreshold) {
		this.useDefaultThreshold = useDefaultThreshold;
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {

		String baseDirectory = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER);
		String wordNet = "RDFGraphLib" + System.getProperty("file.separator") + "WordNet-3.1";
		String wordNetDir  = baseDirectory + System.getProperty("file.separator") + wordNet;
		try{
			wordnet = new RiWordNet(wordNetDir, false, true);
		}catch(RiWordNetError e) {
			throw new RiWordNetError("WordNet path not found:"+wordNetDir + "\n" + e.getMessage());
		}
		
		if(data.length != 2) {
			throw new IllegalArgumentException("Input data does not contain exactly 2 ITableDataFrames");
		}
		
		String table1Header = (String) options.get(0).getSelected();
		String table2Header = (String) options.get(1).getSelected();
		if(table1Header == null) {
			throw new IllegalArgumentException("Table 1 Column Header is not specified under " + COLUMN_ONE_KEY + " in options");
		} 
		if(table2Header == null) {
			throw new IllegalArgumentException("Table 2 Column Header is not specified under " + COLUMN_TWO_KEY + " in options");
		}
		
		ITableDataFrame table1 = data[0];
		ITableDataFrame table2 = data[1];
		
		LOGGER.info("Getting from first table column " + table1Header);
		//NOT IMPLEMENTED YET. TODO switch back when implemented
		Object[] table1Col = table1.getColumn(table1Header);
		//Object[] table1Col = table1.getUniqueValues(table1Header);
		
		LOGGER.info("Getting from second table column " + table2Header);
		Object[] table2Col = table2.getColumn(table2Header);


		boolean col1IsNumeric = table1.isNumeric(table1Header);
		boolean col2IsNumeric = table2.isNumeric(table2Header);
		
		if(col1IsNumeric != col2IsNumeric) {
			throw new IllegalArgumentException("Columns must be of the same type");
		}
		
		//determine the threshold
		if(!useDefaultThreshold) {
			if(col1IsNumeric) {
				threshold = determineMedianMinDistance(table1Col);
			} else{
	//			String[] mostSimilar1 = NaturalLanguageProcessingHelper.determineMostSimilarValues(wordnet, uniqueTable1Col);
	//			threshold = wordnet.getDistance(RiTa.singularize(mostSimilar1[0].toLowerCase()), RiTa.singularize(mostSimilar1[1].toLowerCase()), "n");

				threshold = NaturalLanguageProcessingHelper.determineAverageMinimumSimilarity(wordnet, table1Col);				
//				Object[] uniqueTable2Col = table2.getColumn(options.get(COLUMN_TWO_KEY).toString());	
//				threshold = NaturalLanguageProcessingHelper.determineAverageMinimumSimilarity(wordnet, uniqueTable2Col);
				
			}
		}
		System.out.println("Threshold is: " + threshold);
	
		ITableDataFrame results = performMatch(table1Col, table2Col, col1IsNumeric);
		
		return results;
	}

	private ITableDataFrame performMatch(Object[] table1Col, Object[] table2Col, boolean isNumeric) {		
		String table1ValueKey = "Table1Value";
		String table2ValueKey = "Table2Value";
			
		ITableDataFrame bTree = new BTreeDataFrame(new String[]{table1ValueKey, table2ValueKey});
		
		int success = 0;
		int total = 0;
		
//		boolean[] matched = new boolean[table2Col.length];
//		for(int i=0; i<table2Col.length; i++) {
//			matched[i] = false;
//		}
		
		for(int i = 0; i < table1Col.length; i++) {
			for(int j = 0; j < table2Col.length; j++) {
				Object table1ColVal = table1Col[i];
				Object table2ColVal = table2Col[j];
				boolean match = false;
				
				if(isNumeric) {
					match = approximateMatch(((Double)table1ColVal).doubleValue(),((Double)table2ColVal).doubleValue());
				}else {
					match = approximateMatch(table1ColVal.toString(),table2ColVal.toString());
				}
				
				if(match) {
					System.out.println("APPROX MATCHED::::::::::::::::: " + table1ColVal + "      " + table2ColVal);
					Map<String, Object> row = new HashMap<String, Object>();
					row.put(table1ValueKey , table1ColVal);
					row.put(table2ValueKey, table2ColVal);
					bTree.addRow(row, row); //TODO: adding values as both raw and clean
//					matched[j] = true;
					success++;
				}

				total++;
			}
		}
		
//		//to include all of the unmatched values from col2 if desired
//		if(includeNonMatched) {
//			for(int j = 0; j < flatTable2.size(); j++) {
//				if(!matched[j]) {
//
//					Object[] table2Row = flatTable2.get(j);
//					Object table2ColVal = table2Row[col2Index];
//					System.out.println("NOT MATCHED::::::::::::::::: " + table2ColVal);
//
//					Map<String, Object> row = new HashMap<String, Object>();
//					row.put(col1Name , table2ColVal);
//					for(int k=0; k < table2Headers.length; k++) {
//						if(k != col2Index) {
//							row.put(table2Headers[k], table2Row[k]);
//						}
//					}
//					bTree.addRow(row);
//				}
//			}
//		}
		
		this.resultMetadata.put("success", success);
		this.resultMetadata.put("total", total);
		
		return bTree;
	}
	
//	private ITableDataFrame performMatch(Object[] table1Col, String[] table1Headers, List<Object[]> flatTable2, String[] table2Headers, int col2Index, boolean isNumeric, boolean includeNonMatched) {		
//		for(int i=0; i<table2Headers.length; i++) {
//			String table2Header = table2Headers[i];
//			for(int j=0; j<table1Headers.length; j++) {
//				if(table2Header.equals(table1Headers[j])) {
//					table2Headers[i] = table2Header + "_2";
//				}
//			}
//		}
//
//		String col1Name = options.get(COLUMN_ONE_KEY).toString();
//		String[] bTreeHeaders = new String[table2Headers.length];
//		bTreeHeaders[0] = col1Name;
//		int index = 1;
//		for(int k=0; k < table2Headers.length; k++) {
//			if(k != col2Index) {
//				bTreeHeaders[index] = table2Headers[k];
//				index ++;
//			}
//		}
//		
//		ITableDataFrame bTree = new BTreeDataFrame(bTreeHeaders);
//		
//		int matchCount = 0;
//		int colTotal = 0;
//		boolean[] matched = new boolean[flatTable2.size()];
//		for(int i=0; i<flatTable2.size(); i++) {
//			matched[i] = false;
//		}
//		
//		for(int i = 0; i < table1Col.length; i++) {
//			Object table1ColVal = table1Col[i];
//			for(int j = 0; j < flatTable2.size(); j++) {
//				Object[] table2Row = flatTable2.get(j);
//				Object table2Col = table2Row[col2Index];
//				boolean match = false;
//				
//				if(isNumeric) {
//					match = approximateMatch(((Double)table1ColVal).doubleValue(),((Double)table2Col).doubleValue());
//				}else {
//					match = approximateMatch(table1ColVal.toString(),table2Col.toString());
//				}
//				
//				if(match) {
//					//System.out.println("APPROX MATCHED::::::::::::::::: " + table1ColVal + "      " +   table2Col  );
//					matched[j] = true;
//					matchCount++;
//					Map<String, Object> row = new HashMap<String, Object>();
//					row.put(col1Name , table1ColVal);
//					for(int k=0; k < table2Headers.length; k++) {
//						if(k != col2Index) {
//							row.put(table2Headers[k], table2Row[k]);
//						}
//					}
//					bTree.addRow(row);
//					success++;
//				}
//
//				total++;
//			}
//			colTotal++;
//		}
//		
//		//to include all of the unmatched values from col2 if desired
//		if(includeNonMatched) {
//			for(int j = 0; j < flatTable2.size(); j++) {
//				if(!matched[j]) {
//
//					Object[] table2Row = flatTable2.get(j);
//					Object table2ColVal = table2Row[col2Index];
//					System.out.println("NOT MATCHED::::::::::::::::: " + table2ColVal);
//
//					Map<String, Object> row = new HashMap<String, Object>();
//					row.put(col1Name , table2ColVal);
//					for(int k=0; k < table2Headers.length; k++) {
//						if(k != col2Index) {
//							row.put(table2Headers[k], table2Row[k]);
//						}
//					}
//					bTree.addRow(row);
//				}
//			}
//		}
//
//		System.out.println("approx matched " + matchCount + " out of " + colTotal + " original columns");
//		
//		return bTree;
//	}
	
	private double determineMinDistance(Object[] col) {
		
		double simVal = 2.0;
		
		int i = 0;
		int size = col.length;
		for(; i < size; i++) {
			Double d1 = (Double)col[i];
			int j = i+1;
			for(; j < size; j++) {
				Double d2 = (Double)col[j];
				double newSim = Math.abs(d1 - d2);
				if(newSim < simVal) {
					simVal = newSim;
				}
			}
		}
		
		return simVal;
	}
	
	
	private double determineMedianMinDistance(Object[] col) {
		
		int size = col.length;
		double[] minDistances = new double[size];

		for(int i = 0; i < size; i++) {
			Double d1 = (Double)col[i];
			double simVal = 2.0;
			for(int j = 0; j < size; j++) {
				if(i!=j) {
					Double d2 = (Double)col[j];
					double newSim = Math.abs(d1 - d2);
					if(newSim < simVal) {
						simVal = newSim;
					}
				}
			}
			minDistances[i] = simVal;
		}
		
		double median = StatisticsUtilityMethods.getMedian(minDistances, false);
		
		return median;
	}
	
	private boolean approximateMatch(String obj1, String obj2) {
		double newSim = wordnet.getDistance(RiTa.singularize(obj1.toLowerCase()), RiTa.singularize(obj2.toLowerCase()), "n");
		if(newSim < threshold) {
			System.out.println("APPROX MATCHED::::::::::::::::: " + obj1 + "      " + obj2 +  "      " + newSim);
			
			return true;
		}
		return false;
	}
	
	private boolean approximateMatch(double val1, double val2) {
		if(val2 > val1 - threshold && val2 < val1 + threshold) {
			System.out.println("APPROX MATCHED::::::::::::::::: " + val1 + "      " + val2);
			
			return true;
		}
		return false;
	}
	
	@Override
	public String getName() {
		return "Approximate Object Matcher";
	}

	@Override
	public String getDefaultViz() {
		return null;
	}

	@Override
	public List<String> getChangedColumns() {
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		return this.resultMetadata;
	}

	@Override
	public String getResultDescription() {
		return "This routine matches objects by determining whether they are categorical or numerical and then comparing: 1) in the categorical case, whether they are within a threshold of WordNet similarity and 2) in the numerical case, whether they are within the minimum distance between unique values of column A.";
	}
}
