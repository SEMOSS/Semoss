package prerna.algorithm.learning.moa;

import gov.sandia.cognition.learning.algorithm.perceptron.KernelizableBinaryCategorizerOnlineLearner;
import gov.sandia.cognition.learning.algorithm.perceptron.OnlinePerceptron;
import gov.sandia.cognition.learning.algorithm.perceptron.kernel.KernelBinaryCategorizerOnlineLearnerAdapter;
import gov.sandia.cognition.learning.data.DefaultInputOutputPair;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.function.categorization.DefaultKernelBinaryCategorizer;
import gov.sandia.cognition.learning.function.categorization.LinearBinaryCategorizer;
import gov.sandia.cognition.learning.function.kernel.LinearKernel;
import gov.sandia.cognition.learning.function.kernel.PolynomialKernel;
import gov.sandia.cognition.math.matrix.Vector;
import gov.sandia.cognition.math.matrix.VectorFactory;
import gov.sandia.cognition.math.matrix.mtj.DenseVector;
import gov.sandia.cognition.math.matrix.mtj.Vector2;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import weka.core.Utils;

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

//		List<Object[]> dataTable = table.getScaledData();
//		Collections.shuffle(dataTable);

		
		ITableDataFrame table = data[0];
		int numAttributes = table.getNumCols();
		String[] names = table.getColumnHeaders();		

		//List<Object[]> dataTable = table.getScaledData();
		List<Object[]> dataTable = table.getData();
		//Collections.shuffle(dataTable);
		
		
		
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
		Instances instanceData = WekaUtilityMethods.createInstancesFromQuery("DataSet", dataTable, names, classIndex);
		instanceData.setClassIndex(classIndex);

		//make a header
		InstancesHeader header = new InstancesHeader(instanceData);
		learner.setModelContext(header);
		learner.prepareForUse();

//		List<Object[]> dataTable = table.getScaledData();
//		Collections.shuffle(dataTable);
		int numIterations = 10;
		String[] correctArray = new String[dataTable.size()];
		this.testKernelPerceptron(dataTable, instanceData, isCategorical, numAttributes);
		//for(int x = 1; x <= numIterations; x++) {
			int correct = 0;
			int total = dataTable.size();
			double ratio;
			for(int i = 0; i < dataTable.size(); i++) {
	
				Object[] newRow = dataTable.get(i);//iterator.next();
				Instance nextInst = WekaUtilityMethods.createInstance(instanceData, newRow, isCategorical, numAttributes);	
				Instance abc = nextInst;
				//System.out.println(Arrays.toString(nextInst.toDoubleArray()));
				//System.out.println(newRow[classIndex]);
				//System.out.println(nextInst.toString());
				//Instance nextInst = WekaUtilityMethods.createInstance(null, iterator.next(), isCategorical, numAttributes - 1);
				//double[] votes = learner.getVotesForInstance(nextInst);
				//System.out.println(Arrays.toString(votes));
				//System.out.println(Arrays.toString(newRow));
				
				Boolean correctlyClassified = learner.correctlyClassifies(nextInst);
				if(correctlyClassified) {
					correctArray[i] = correctlyClassified.toString();
					correct++;
				} else {
					int index = Utils.maxIndex(learner.getVotesForInstance(nextInst));
					String className = learner.getClassLabelString(index);
					className = className.substring(1, className.length()-1);
					int beginIndex = className.indexOf(":");
					className = className.substring(beginIndex+1);
					correctArray[i] = className;
				}
				
				//System.out.println("AttributeNameString"+learner.getAttributeNameString(classIndex));
				//System.out.println("ClassLabelString"+learner.getClassLabelString(0));
				
				//System.out.println(nextInst.classValue());
				//System.out.println(nextInst.value(0));
				//System.out.println(nextInst.stringValue(0));
				//Attribute a = System.out.println(nextInst.)
				
//				if(correctlyClassified) {
//					correct++;
//				} else {
//					int index = Utils.maxIndex(learner.getVotesForInstance(nextInst));
//					String className = learner.getClassLabelString(index);
//					System.out.println(className);
//				}

//				Map<String, Object> nextRow = new HashMap<>();
//				nextRow.put(newNames[0], newRow[0]);
//				nextRow.put("Correctly_Classified", correctlyClassified.toString());
//				newTable.addRow(nextRow, nextRow);

				learner.trainOnInstance(nextInst);
			}
			ratio = 100.0*(double)correct/(double)total;
	//}
		String columnName = "Correctly_Classified -- "+(int)ratio+"%";
		String[] newNames = {names[0], columnName};
		ITableDataFrame newTable = new BTreeDataFrame(newNames);
		for(int i = 0; i < dataTable.size(); i++) {
			Map<String, Object> nextRow = new HashMap<>();
			nextRow.put(newNames[0], dataTable.get(i)[0]);
			nextRow.put(columnName, correctArray[i]);
			newTable.addRow(nextRow, nextRow);
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
			Object[] row = iterator.next();
			Instance trainInst = WekaUtilityMethods.createInstance(instanceData, row, isCategorical, trainingSet.getColumnHeaders().length - 1);
			System.out.println(trainInst.toDoubleArray());
			System.out.println(row[classIndex]);
			learner.trainOnInstanceImpl(trainInst);
		}
	}

    private void testKernelPerceptron(List<Object[]> allData, Instances instanceData, boolean[] isCategorical, int numAttributes) {
    	
    	KernelBinaryCategorizerOnlineLearnerAdapter<Vector> instance0 = new KernelBinaryCategorizerOnlineLearnerAdapter<Vector>(new PolynomialKernel(3, 1.0), new OnlinePerceptron());
        KernelBinaryCategorizerOnlineLearnerAdapter<Vector> instance = new KernelBinaryCategorizerOnlineLearnerAdapter<Vector>(new PolynomialKernel(2, 1.0), new OnlinePerceptron());
        KernelBinaryCategorizerOnlineLearnerAdapter<Vector> instance2 = new KernelBinaryCategorizerOnlineLearnerAdapter<Vector>(new LinearKernel(), new OnlinePerceptron());
        
        DefaultKernelBinaryCategorizer<Vector> learned0 = instance0.createInitialLearnedObject();
        DefaultKernelBinaryCategorizer<Vector> learned = instance.createInitialLearnedObject();
        DefaultKernelBinaryCategorizer<Vector> learned2 = instance2.createInitialLearnedObject();
        
        int correctCount0 = 0;
        int correctCount = 0;
        int correctCount2 = 0;
        
        int total = 100;
        Collections.shuffle(allData);
    	for(int z = 0; z < allData.size(); z++) {
    		
			Object[] newRow = allData.get(z);
			Instance nextInst = WekaUtilityMethods.createInstance(instanceData, newRow, isCategorical, numAttributes);
	    	double[] array = nextInst.toDoubleArray();
	    	double[] array2 = new double[array.length-1];
	    	int b = 0;
	    	for(int a = 0; a < array.length; a++) {
	    		if(a!=3) {
	    			array2[b] = array[a];
	    			b++;
	    		}
	    	}
	
	    	Vector v = VectorFactory.getDenseDefault().copyArray(array2);
	    	boolean bool = newRow[3].toString().equalsIgnoreCase("Y") ? true : false;
	    	InputOutputPair<Vector, Boolean> example = DefaultInputOutputPair.create(v, bool);

	        if(z < total) {
	        	this.applyUpdate(instance0, learned0, example);
	            this.applyUpdate(instance, learned, example);
	            this.applyUpdate(instance2, learned2, example);
	        } else {

	        	//polynomial degree 3
	        	boolean actual0 = example.getOutput();
	        	boolean predicted0 = learned0.evaluate(example.getInput());
	        	if(actual0==predicted0) {
	        		correctCount0++;
	        	}
	        	
	        	//polynomial degree 2
	            boolean actual = example.getOutput();
	            boolean predicted = learned.evaluate(example.getInput());
	            if (actual == predicted) {
	                correctCount++;
	            }

	            //linear
	            boolean actual2 = example.getOutput();
	            boolean predicted2 = learned2.evaluate(example.getInput());
	            if (actual2 == predicted2) {
	                correctCount2++;
	            }
	        }
    	}
    	double accuracy0 = (double) correctCount0 / (allData.size() - total);
        double accuracy = (double) correctCount / (allData.size()-total);
        double accuracy2 = (double) correctCount2/(allData.size()-total);
        
        System.out.println("Accuracy for polynomial perceptron, degree 3: "+accuracy0);
        System.out.println("Accuracy for polynomial perceptron, degree 2: " + accuracy);
        System.out.println("Accuracy for linear perceptron: " + accuracy2);
        System.out.println();
    }
	
    protected void applyUpdate(final KernelizableBinaryCategorizerOnlineLearner learner, final LinearBinaryCategorizer target, final InputOutputPair<Vector, Boolean> example) {
    	learner.update(target, example);
    }

    protected void applyUpdate(final KernelBinaryCategorizerOnlineLearnerAdapter<Vector> learner, final DefaultKernelBinaryCategorizer<Vector> target, final InputOutputPair<Vector, Boolean> example) {
    	learner.update(target, example);
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
