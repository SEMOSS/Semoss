package prerna.algorithm.cluster;

public abstract class AbstractNumericalMethods {
	
	protected String[][] numericalBinMatrix;
	protected String[][] categoricalMatrix;
	protected String[][] instanceNumberBinOrderingMatrix;
	protected int numericPropNum;
	protected int categoricalPropNum;
	protected int totalPropNum;
	
	protected double[] numericalWeights;
	protected double[] categoricalWeights;
	
	public AbstractNumericalMethods(String[][] numericalBinMatrix, String[][] categoricalMatrix, String[][] instanceNumberBinOrderingMatrix) {
		this.numericalBinMatrix = numericalBinMatrix;
		this.categoricalMatrix = categoricalMatrix;
		this.instanceNumberBinOrderingMatrix = instanceNumberBinOrderingMatrix;
		
		if(numericalBinMatrix != null) {
			numericPropNum = numericalBinMatrix[0].length;
		}
		if(categoricalMatrix != null) {
			categoricalPropNum = categoricalMatrix[0].length;
		}
		totalPropNum = numericPropNum + categoricalPropNum;
	}
	
	public void setCategoricalWeights(double[] categoricalWeights) {
		this.categoricalWeights = categoricalWeights;
	}

	public void setNumericalWeights(double[] numericalWeights) {
		this.numericalWeights = numericalWeights;
	}
}
