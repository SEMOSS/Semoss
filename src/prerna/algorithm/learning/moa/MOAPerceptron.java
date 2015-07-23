package prerna.algorithm.learning.moa;

import moa.classifiers.functions.Perceptron;
import weka.core.Instance;

public class MOAPerceptron extends Perceptron {

	@Override
     public void trainOnInstanceImpl(Instance inst) {
 
		//Init Perceptron
		if (this.reset == true) {
			this.reset = false;
			this.numberAttributes = inst.numAttributes();
			this.numberClasses = inst.numClasses();
			this.weightAttribute = new double[inst.numClasses()][inst.numAttributes()];
			for (int i = 0; i < inst.numClasses(); i++) {
				for (int j = 0; j < inst.numAttributes(); j++) {
					weightAttribute[i][j] = 0.2 * Math.random() - 0.1;
				}
			}
		}

		double[] preds = new double[inst.numClasses()];
			for (int i = 0; i < inst.numClasses(); i++) {
				preds[i] = prediction(inst, i);
			}
    
		double learningRatio = learningRatioOption.getValue();

		int actualClass = (int) inst.classValue();
		for (int i = 0; i < inst.numClasses(); i++) {
			//Abstract this so it can be interchangeable
			double actual = (i == actualClass) ? 1.0 : 0.0;
			double delta = (actual - preds[i]) * preds[i] * (1 - preds[i]);
			for (int j = 0; j < inst.numAttributes() - 1; j++) {
				this.weightAttribute[i][j] += learningRatio * delta * inst.value(j);
			}
			this.weightAttribute[i][inst.numAttributes() - 1] += learningRatio * delta;
		}
	}
	
	@Override
	//Abstract this so it can be defined elsewhere
     public double prediction(Instance inst, int classVal) {
         double sum = 0.0;
         for (int i = 0; i < inst.numAttributes() - 1; i++) {
             sum += weightAttribute[classVal][i] * inst.value(i);
         }
         sum += weightAttribute[classVal][inst.numAttributes() - 1];
         return 1.0 / (1.0 + Math.exp(-sum));
     }

	
}
