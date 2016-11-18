package prerna.algorithm.learning.moa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import gov.sandia.cognition.learning.algorithm.perceptron.KernelizableBinaryCategorizerOnlineLearner;
import gov.sandia.cognition.learning.algorithm.perceptron.OnlinePerceptron;
import gov.sandia.cognition.learning.algorithm.perceptron.kernel.KernelBinaryCategorizerOnlineLearnerAdapter;
import gov.sandia.cognition.learning.algorithm.perceptron.kernel.OnlineKernelPerceptron;
import gov.sandia.cognition.learning.data.DefaultInputOutputPair;
import gov.sandia.cognition.learning.data.InputOutputPair;
import gov.sandia.cognition.learning.function.categorization.BinaryVersusCategorizer;
import gov.sandia.cognition.learning.function.categorization.BinaryVersusCategorizer.Learner;
import gov.sandia.cognition.learning.function.categorization.DefaultKernelBinaryCategorizer;
import gov.sandia.cognition.learning.function.categorization.LinearBinaryCategorizer;
import gov.sandia.cognition.learning.function.kernel.ExponentialKernel;
import gov.sandia.cognition.learning.function.kernel.Kernel;
import gov.sandia.cognition.learning.function.kernel.LinearKernel;
import gov.sandia.cognition.learning.function.kernel.PolynomialKernel;
import gov.sandia.cognition.learning.function.kernel.SigmoidKernel;
import gov.sandia.cognition.math.matrix.Vector;
import gov.sandia.cognition.math.matrix.VectorFactory;
import moa.classifiers.functions.Perceptron;
import moa.core.InstancesHeader;
import prerna.algorithm.api.IAnalyticTransformationRoutine;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.learning.weka.WekaUtilityMethods;
import prerna.ds.h2.H2Frame;
import prerna.om.SEMOSSParam;
import weka.core.Attribute;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;

public class MOAPerceptronRunner implements IAnalyticTransformationRoutine {
	protected List<SEMOSSParam> options;
	private Perceptron learner;
	private String className;
	private double[][] weights;
	private double accuracy;
	private int classIndex;
	private boolean isNumericBucket;
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
		
		SEMOSSParam p3 = new SEMOSSParam();
		p3.setName("KernelType");
		options.add(2, p3);

		SEMOSSParam p4 = new SEMOSSParam();
		p4.setName("degree");
		options.add(3, p4);
		
		SEMOSSParam p5 = new SEMOSSParam();
		p5.setName("kappa");
		options.add(4, p5);
		
		SEMOSSParam p6 = new SEMOSSParam();
		p6.setName("constant");
		options.add(5, p6);
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
		accuracy = 0.0;
		isNumericBucket = false;
		className = (String)this.options.get(0).getSelected();
		String kernelType = (String)this.options.get(2).getSelected();
		Integer degree = (Integer)this.options.get(3).getSelected();
		if(degree==null) degree = 2;
		Double kappa = (Double)this.options.get(4).getSelected();
		if(kappa == null) kappa = 1.0;
		Double constant = (Double)this.options.get(5).getSelected();
		
		ITableDataFrame table = data[0];
		//ArrayList<String> skip = new ArrayList<>();
		//skip.add(table.getColumnHeaders()[0]);
		//table.setColumnsToSkip(skip);
		String[] names = table.getColumnHeaders();
		int numAttributes = names.length;

		List<Object[]> dataTable;
		if(kernelType.equalsIgnoreCase("Exponential")) {
			List<String> exceptionColumns = new ArrayList<String>();
			exceptionColumns.add(names[classIndex]);
			dataTable = table.getScaledData(exceptionColumns);
		} else {
			dataTable = table.getData();//Data();
		}
		//Collections.shuffle(dataTable);
		
		
		for(classIndex = 0; classIndex < names.length; classIndex++) {
			if(names[classIndex].equals(className)) {
				break;
			}
		}

		boolean[] isCategorical = table.isNumeric();
		if(isCategorical[classIndex]) {
			isNumericBucket = true;
		}
		
//		for(int i = 0; i < isCategorical.length; i++) {
//			isCategorical[i] = !isCategorical[i]; 
//		}
		
		boolean classIndexCategorical = !table.isNumeric(names[classIndex]);
		Instances instanceData = WekaUtilityMethods.createInstancesFromQuery("DataSet", dataTable, names, classIndex);
		for(int i = 0; i < instanceData.numAttributes(); i++) {
			isCategorical[i] = !instanceData.attribute(i).isNumeric();
		}
		isCategorical[classIndex] = classIndexCategorical;
		
		instanceData.setClassIndex(classIndex);
	
		String[] correctArray = null;
		if(kernelType.equalsIgnoreCase("MOA Linear")) {
			correctArray = this.runMOALinearPerceptron(dataTable, instanceData, isCategorical, numAttributes);
		} else if(kernelType.equalsIgnoreCase("Linear")) {
			Kernel kernel = new LinearKernel().getInstance();
			correctArray = this.runKernelPerceptron(dataTable, instanceData, isCategorical, numAttributes, kernel);
		} else if(kernelType.equalsIgnoreCase("Polynomial")) {
			Kernel kernel = new PolynomialKernel(degree, constant);
			correctArray = this.runKernelPerceptron(dataTable, instanceData, isCategorical, numAttributes, kernel);
		} else if(kernelType.equalsIgnoreCase("Exponential")) {
			Kernel kernel = new ExponentialKernel(LinearKernel.getInstance());
			correctArray = this.runKernelPerceptron(dataTable, instanceData, isCategorical, numAttributes, kernel);
		} else if(kernelType.equalsIgnoreCase("Sigmoid")) {
			Kernel kernel = new SigmoidKernel(kappa, constant);
			correctArray = this.runKernelPerceptron(dataTable, instanceData, isCategorical, numAttributes, kernel);
		}
		
		String columnName = "Correctly_Classified -- "+(int)accuracy+"%";
		//table.setColumnsToSkip(null);
		String[] newNames = {names[0], columnName};
		ITableDataFrame newTable = new H2Frame(newNames);
		for(int i = 0; i < dataTable.size(); i++) {
			Map<String, Object> nextRow = new HashMap<>();
			nextRow.put(newNames[0], dataTable.get(i)[0]);
			nextRow.put(columnName, correctArray[i]);
			newTable.addRow(nextRow);
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

		Iterator<Object[]> iterator = trainingSet.scaledIterator();
		while(iterator.hasNext()) {
			Object[] row = iterator.next();
			Instance trainInst = WekaUtilityMethods.createInstance(instanceData, row, isCategorical, trainingSet.getColumnHeaders().length - 1);
			System.out.println(trainInst.toDoubleArray());
			System.out.println(row[classIndex]);
			learner.trainOnInstanceImpl(trainInst);
		}
	}

	private String[] runMOALinearPerceptron(List<Object[]> dataTable, Instances instanceData, boolean[] isCategorical, int numAttributes) {
		//make a header
		InstancesHeader header = new InstancesHeader(instanceData);
		learner.setModelContext(header);
		learner.prepareForUse();

		String[] correctArray = new String[dataTable.size()];
		
		int correct = 0;
		int total = dataTable.size();

		for(int i = 0; i < dataTable.size(); i++) {

			Object[] newRow = dataTable.get(i);
			Instance nextInst = WekaUtilityMethods.createInstance(instanceData, newRow, isCategorical, numAttributes);	
			
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

			learner.trainOnInstance(nextInst);
		}
		accuracy = 100.0*((double)correct/(double)total);
		
		return correctArray;
	}
    private String[] runKernelPerceptron(List<Object[]> allData, Instances instanceData, boolean[] isCategorical, int numAttributes, Kernel kernel) {
       
        int numCategories = instanceData.attribute(classIndex).numValues();
        String[] classes = new String[numCategories];
        for(int i = 0; i < numCategories; i++) {
        	classes[i] = instanceData.attribute(classIndex).value(i);
        }
        KernelBinaryCategorizerOnlineLearnerAdapter<Vector> instance[] = new KernelBinaryCategorizerOnlineLearnerAdapter[numCategories];
        DefaultKernelBinaryCategorizer<Vector> learned[] = new DefaultKernelBinaryCategorizer[numCategories];
        DefaultInputOutputPair<Vector, Boolean> example[] = new DefaultInputOutputPair[numCategories];
        double[] outputValues = new double[numCategories];
        
        for(int i = 0; i < numCategories; i++) {
        	instance[i] = new KernelBinaryCategorizerOnlineLearnerAdapter<Vector>(kernel, new OnlinePerceptron());
        	learned[i] = instance[i].createInitialLearnedObject();
        	//instance[i].
        }
        
        int correctCount = 0;
        String[] correctArray = new String[allData.size()];
        
        for(int z = 0; z < allData.size(); z++) {
			
        	//Create the input vector
			Object[] newRow = allData.get(z);		
			Instance nextInst = WekaUtilityMethods.createInstance(instanceData, newRow, isCategorical, numAttributes);
	    	double[] array = nextInst.toDoubleArray();
	    	double[] array2 = new double[array.length-2];
	    	int b = 0;
	    	for(int a = 1; a < array.length; a++) {
	    		if(a!=classIndex) {
	    			array2[b] = array[a];
	    			b++;
	    		}
	    	}
	    	Vector v = VectorFactory.getDenseDefault().copyArray(array2);
	    	
	    	String attribute = newRow[classIndex].toString();
	    	for(int i = 0; i < numCategories; i++) {
	    		if(isCategorical[classIndex]) {
		    		if(attribute.equalsIgnoreCase(classes[i])) {
		    			example[i] = DefaultInputOutputPair.create(v, true);
		    		} else {
		    			example[i] = DefaultInputOutputPair.create(v, false);
		    		}
	    		} else {
	    			if(inBucket(newRow[classIndex], classes[i])) {
	    				example[i] = DefaultInputOutputPair.create(v, true);
	    			} else {
	    				example[i] = DefaultInputOutputPair.create(v, false);
	    			}
	    		}
	    		
	    		outputValues[i] = learned[i].evaluateAsDouble(v);
	    		//Boolean val = learned[i].evaluate(v);
	    		//String val2 = learned[i].evaluate(v);
	    	}
	        
	    	int maxIndex = 0;
	    	Double maxValue = Double.NEGATIVE_INFINITY;
	    	for(int i = 0; i < numCategories; i++) {
	    		if(outputValues[i] > maxValue) {
	    			maxIndex = i;
	    			maxValue = outputValues[i];
	    		}
	    	}
	    	
	    	if(isCategorical[classIndex] && !isNumericBucket) {
		    	if(classes[maxIndex].equalsIgnoreCase(attribute)) {
		    		correctCount++;
		    		correctArray[z] = "true";
		    	} else {
		    		correctArray[z] = classes[maxIndex];
		    	}
	    	} else {
		    	if(inBucket(attribute, classes[maxIndex])) {
		    		correctCount++;
		    		correctArray[z] = "true";
		    	} else {
		    		correctArray[z] = classes[maxIndex];
		    	}
	    	}
	    	
	        this.applyUpdate(instance, learned, example);
		}
		accuracy =  100.0 * ((double)correctCount / (double)(allData.size()));
		return correctArray;
    }
	
    private void applyUpdate(final KernelizableBinaryCategorizerOnlineLearner learner, final LinearBinaryCategorizer target, final InputOutputPair<Vector, Boolean> example) {
    	learner.update(target, example);
    }

    private void applyUpdate(final KernelBinaryCategorizerOnlineLearnerAdapter<Vector> learner[], final DefaultKernelBinaryCategorizer<Vector>[] target, final InputOutputPair<Vector, Boolean>[] example) {
    	for(int i = 0; i < learner.length; i++) {
    		learner[i].update(target[i], example[i]);
    	}
    }
    
    private String[] runMultiClassKernelPerceptron(List<Object[]> allData, Instances instanceData, boolean[] isCategorical, int numAttributes, Kernel kernel) {
		
        KernelBinaryCategorizerOnlineLearnerAdapter<Vector> instance = new KernelBinaryCategorizerOnlineLearnerAdapter<Vector>(kernel, new OnlinePerceptron());
        DefaultKernelBinaryCategorizer<Vector> learned = instance.createInitialLearnedObject();    
        int correctCount = 0;
        String[] correctArray = new String[allData.size()];
        
        ArrayList<InputOutputPair<Vector, String>> collection = new ArrayList<>();
        
		for(int z = 0; z < allData.size(); z++) {
			String classCat = instanceData.attribute(classIndex).value(0);
			Object[] newRow = allData.get(z);
			
			Instance nextInst = WekaUtilityMethods.createInstance(instanceData, newRow, isCategorical, numAttributes);
	    	double[] array = nextInst.toDoubleArray();
	    	double[] array2 = new double[array.length-2];
	    	int b = 0;
	    	for(int a = 1; a < array.length; a++) {
	    		if(a!=classIndex) {
	    			array2[b] = array[a];
	    			b++;
	    		}
	    	}
	
	    	Vector v = VectorFactory.getDenseDefault().copyArray(array2);
	    	String attribute = newRow[classIndex].toString();
	    	if(!isCategorical[classIndex]) {
	    		attribute = determineBucket(newRow[classIndex], instanceData.attribute(classIndex));
	    	}
	    	InputOutputPair<Vector, String> example = DefaultInputOutputPair.create(v, attribute);
	    	collection.add(example);
		}

		Learner<Vector, String> categorizer = new BinaryVersusCategorizer.Learner<Vector, String>();
		//OnlineKernelPerceptron<Vector> p = new OnlineKernelPerceptron<Vector>(new PolynomialKernel(degree, constant));
		
		categorizer.setLearner(new OnlineKernelPerceptron<Vector>(kernel));	
		BinaryVersusCategorizer<Vector, String> b = categorizer.learn(collection);
		
		
		for(int i = 0; i < collection.size(); i++) {
	        String actual = collection.get(i).getOutput();
	        
	        String predicted = b.evaluate(collection.get(i).getInput());
	        Boolean result = actual.equalsIgnoreCase(predicted);
	        if (result) {
	            correctCount++;
	            correctArray[i] = result.toString();
	        } else {
	        	correctArray[i] = predicted;
	        }
	        
	        //System.out.println(predicted+ "   "+result);
	        
	        //correctArray[z] = result.toString();//(actual==predicted).toString();
	        //this.applyUpdate(instance, learned, example);
		}
		//System.out.println(correctCount);
		accuracy = (double) correctCount / (allData.size())*100;
		return correctArray;
    }
    
    private String determineBucket(Object objValue, Attribute buckets) {
    	double value = 0.0;
    	try {
    		value = ((Number)objValue).doubleValue();
    	} catch(Exception e) {
    		return buckets.value(0);
    	}
    	for(int i = 0; i < buckets.numValues(); i++) {
    		String s = buckets.value(i);
    		String[] values = s.split(" - ");
    		Double min = Double.parseDouble(values[0]);
    		Double max = Double.parseDouble(values[1]);
    		if(value >= min && value <= max) {
    			return s;
    		}
    	}
    	
    	return "";
    }
    
    private boolean inBucket(Object objValue, String bucket) {
    	double value = 0.0;
    	try {
    		if(objValue instanceof String) {
    			value = Double.parseDouble((String)objValue);
    		} else {
    			value = ((Number)objValue).doubleValue();
    		}
    	} catch(Exception e) {
    		return false;
    	}
    	boolean returnVal = false;
    	String[] values = bucket.split(" - ");
		Double min = Double.parseDouble(values[0]);
		Double max = Double.parseDouble(values[1]);
		if(value >= min && value <= max) {
			returnVal = true;
		} 
		return returnVal;
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
