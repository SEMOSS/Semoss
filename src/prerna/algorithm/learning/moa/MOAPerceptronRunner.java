package prerna.algorithm.learning.moa;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import moa.classifiers.Classifier;
import moa.classifiers.functions.Perceptron;
import moa.core.InstancesHeader;
import moa.options.FloatOption;
import moa.streams.ArffFileStream;

import org.supercsv.cellprocessor.Optional;
import org.supercsv.cellprocessor.ift.CellProcessor;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import prerna.algorithm.api.IAnalyticRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.weka.WekaUtilityMethods;
import prerna.ds.BTreeDataFrame;
import prerna.ds.BTreeIterator;
import prerna.ds.ValueTreeColumnIterator;
import prerna.error.FileReaderException;
import prerna.math.BarChart;
import prerna.om.SEMOSSParam;
import prerna.util.Utility;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class MOAPerceptronRunner implements IAnalyticRoutine {
	protected List<SEMOSSParam> options;
	private Perceptron learner;
	private String className;
	private double[][] weights;
	//private double[][] weightAttribute;
	//private FloatOption learningRatioOption = new FloatOption("learningRatio", 'r', "Learning ratio", 1);

	public MOAPerceptronRunner() {
		learner = new MOAPerceptron();

		this.options = new ArrayList<SEMOSSParam>();

		SEMOSSParam p1 = new SEMOSSParam();
		p1.setName("className");
		options.add(0, p1);

		SEMOSSParam p2 = new SEMOSSParam();
		p2.setName("skipAttributes");
		options.add(1, p2);

		//add weights option
	}

	public double[][] getWeights() {
		return ((MOAPerceptron)learner).getWeights();
	}

	@Override
	public String getName() {
		return "MOA Perceptron";
	}

	@Override
	public String getResultDescription() {
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
		return options;
	}

	@Override
	public ITableDataFrame runAlgorithm(ITableDataFrame... data) {
		className = (String)this.options.get(0).getSelected();
		//need to set weights
		//need to set learningRatioOption


		ITableDataFrame table = data[0];
		int numAttributes = table.getNumCols();
		String[] names = table.getColumnHeaders();		

		String[] newNames = {names[0], "Correctly_Classified"};
		ITableDataFrame newTable = new BTreeDataFrame(newNames);
		
		int classIndex;
		for(classIndex = 0; classIndex < names.length; classIndex++) {
			if(names[classIndex].equals(className)) {
				break;
			}
		}

		if(data.length > 1) {
			ITableDataFrame trainingTable = data[1];
			this.trainClassifier(trainingTable, classIndex);
		}

		boolean[] isCategorical = table.isNumeric();
		for(int i = 0; i < isCategorical.length; i++) {
			isCategorical[i] = !isCategorical[i]; 
		}

		Iterator<Object[]> iterator = table.scaledIterator(false);
		//Iterator<Object[]> iterator = table.iterator(false);
		Instances instanceData = WekaUtilityMethods.createInstancesFromQuery("DataSet", (ArrayList<Object[]>)table.getScaledData(), names, classIndex);
		instanceData.setClassIndex(classIndex);

		//make a header
		InstancesHeader header = new InstancesHeader(instanceData);
		learner.setModelContext(header);
		learner.prepareForUse();

		while(iterator.hasNext()) {
			Object[] newRow = iterator.next();
			Instance nextInst = WekaUtilityMethods.createInstance(instanceData, newRow, isCategorical, numAttributes - 1);
			//Instance nextInst = WekaUtilityMethods.createInstance(null, iterator.next(), isCategorical, numAttributes - 1);
			//double[] votes = learner.getVotesForInstance(nextInst);
			//System.out.println(Arrays.toString(votes));
			Boolean correctlyClassified = learner.correctlyClassifies(nextInst);
			Map<String, Object> nextRow = new HashMap<>();
			nextRow.put(newNames[0], newRow[0]);
			nextRow.put("Correctly_Classified", correctlyClassified.toString());
			newTable.addRow(nextRow, nextRow);
			learner.trainOnInstance(nextInst);
		}

		return newTable;
	}

	private void trainClassifier(ITableDataFrame trainingSet, int classIndex) {
		boolean[] isCategorical = trainingSet.isNumeric();

		for(int i = 0; i < isCategorical.length; i++) {
			isCategorical[i] = !isCategorical[i];
		}

		Instances instanceData = WekaUtilityMethods.createInstancesFromQuery("TrainingSet", (ArrayList<Object[]>)trainingSet.getScaledData(), trainingSet.getColumnHeaders(), classIndex);
		instanceData.setClassIndex(classIndex);

		InstancesHeader header = new InstancesHeader(instanceData);
		learner.setModelContext(header);
		learner.prepareForUse();

		Iterator<Object[]> iterator = trainingSet.scaledIterator(false);
		while(iterator.hasNext()) {
			Instance trainInst = WekaUtilityMethods.createInstance(instanceData, iterator.next(), isCategorical, trainingSet.getColumnHeaders().length - 1);
			learner.trainOnInstanceImpl(trainInst);
		}
	}

	@Override
	public String getDefaultViz() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<String> getChangedColumns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Object> getResultMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
