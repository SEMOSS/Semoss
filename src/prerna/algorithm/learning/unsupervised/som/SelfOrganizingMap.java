package prerna.algorithm.learning.unsupervised.som;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import prerna.algorithm.learning.similarity.ClusterCenter;
import prerna.algorithm.learning.similarity.ClusterCenterNumericalMethods;
import prerna.algorithm.learning.similarity.ClusteringDataProcessor;

public class SelfOrganizingMap {

	private SelfOrganizingMapGrid grid;
	private int length;
	private int height;
	private int numGrids;
	
	private ClusterCenterNumericalMethods cnm;
	private List<ClusterCenter> gridCategoricalValues;
	private List<ClusterCenter> gridNumericalBinValues;
	
	private int numInstances;
	private int maxInstanceSize = 3000;
	private String[][] instanceNumberBinMatrix;
	private String[][] instanceCategoryMatrix;
	private String[][] instanceNumberBinOrderingMatrix;
	private double[] numericalWeights;
	private double[] categoricalWeights;
	
	private int maxIt = 15;
	private double l0 = 0.07;
	private double r0;
	private double tau;
	
	private int[] gridAssignmentForInstances;
	private int[] numInstancesInGrid;
	
	public SelfOrganizingMap() {
		this.grid = new SelfOrganizingMapGrid();
		this.gridCategoricalValues = new ArrayList<ClusterCenter>();
		this.gridNumericalBinValues = new ArrayList<ClusterCenter>();
	}
	
	public SelfOrganizingMap(ArrayList<Object[]> queryData, String[] varNames) {
		this.grid = new SelfOrganizingMapGrid();
		this.numInstances = queryData.size();
		setGridSize(numInstances, maxInstanceSize);
		
		r0 = (double) length / 6;
		tau = (double) maxIt / r0;
		
		this.gridCategoricalValues = new ArrayList<ClusterCenter>(numGrids);
		fillEmptyGrids(this.gridCategoricalValues, numGrids);
		this.gridNumericalBinValues = new ArrayList<ClusterCenter>(numGrids);
		fillEmptyGrids(this.gridNumericalBinValues, numGrids);
		
		ClusteringDataProcessor cdp = new ClusteringDataProcessor(queryData, varNames);
		this.instanceCategoryMatrix = cdp.getCategoricalMatrix();
		this.instanceNumberBinMatrix = cdp.getNumericalBinMatrix();
		this.instanceNumberBinOrderingMatrix = cdp.getNumericalBinOrderingMatrix();
		this.categoricalWeights = cdp.getCategoricalWeights();
		this.numericalWeights = cdp.getNumericalWeights();
		
		this.cnm = new ClusterCenterNumericalMethods(instanceNumberBinMatrix, instanceCategoryMatrix, instanceNumberBinOrderingMatrix);
		cnm.setCategoricalWeights(categoricalWeights);
		cnm.setNumericalWeights(numericalWeights);
	}
	
	public boolean execute() {
		gridAssignmentForInstances = new int[numInstances];
		setValuesToNegativeOne(gridAssignmentForInstances);
		numInstancesInGrid = new int[numGrids];
		
		boolean success = true;
		
		// randomly add instances to grid
		Random randomGenerator = new Random();
		int idx = 0;
		for(; idx < numGrids; idx++) {
			// get random instance not placed yet
			int getRandomInstance = randomGenerator.nextInt(numInstances);
			while(gridAssignmentForInstances[getRandomInstance] != -1) {
				getRandomInstance = randomGenerator.nextInt(numInstances);
			}
			
			// get random grid without any instances inside
			int getRandomGrid = randomGenerator.nextInt(numGrids);
			while(numInstancesInGrid[getRandomGrid] != 0) {
				getRandomGrid = randomGenerator.nextInt(numGrids);
			}
			
			ClusterCenter numericalClusterCenter = gridNumericalBinValues.get(getRandomGrid);
			ClusterCenter categoricalClusterCenter = gridCategoricalValues.get(getRandomGrid);
			cnm.addToClusterCenter(getRandomInstance, numericalClusterCenter, categoricalClusterCenter);

			gridAssignmentForInstances[getRandomInstance] = getRandomGrid;
			numInstancesInGrid[getRandomGrid]++;
		}
		
		int currIt = 0;
		// base radius
		boolean change = true;
		while(currIt < maxIt && change) {
			System.out.println("Current Iteration: " + currIt);
			change = false;
			int i = 0;
			// determine radius of influence for this iteration
			double radiusOfInfluence = r0 * Math.exp( -1.0 * currIt / tau);
			// determine learning rate for this iteration
			double learningInfluence = l0 * Math.exp( -1.0 * currIt / tau);
			for(; i < numInstances; i++) {
				// find optimal cell for instance
				int gridIndex = determineMostSimilarGridForInstance(i);
				// check instance is changing grid locations
				if(gridAssignmentForInstances[i] != gridIndex) {
					change = true;
				
					cnm.addToClusterCenter(i, gridNumericalBinValues.get(gridIndex), gridCategoricalValues.get(gridIndex));
					numInstancesInGrid[gridIndex]++;
					
					// if instance was in a previous cell, update that cell
					int oldInstanceGridInde = gridAssignmentForInstances[i];
					if(oldInstanceGridInde != -1) {
//						cnm.removeFromClusterCenter(i, gridNumericalBinValues.get(oldInstanceGridInde), gridCategoricalValues.get(oldInstanceGridInde));
						numInstancesInGrid[oldInstanceGridInde]--;
					}
					
					// update the cells surrounding the main cell
					Map<String, List<Integer>> neighborhoodEffectHash = grid.getAdjacentCellsInRadius(gridIndex, radiusOfInfluence);
					List<Integer> adjacentCells = neighborhoodEffectHash.get(SelfOrganizingMapGrid.ADJACENT_CELLS_KEY);
					List<Integer> adjacentCellsRadius = neighborhoodEffectHash.get(SelfOrganizingMapGrid.ADJACENT_CELLS_RADIUS_KEY);
					int adjIdx = 0;
					int adjSize = adjacentCells.size();
					for(; adjIdx < adjSize; adjIdx++) {
						int effected_grid = adjacentCells.get(adjIdx);
						int effect_radius = adjacentCellsRadius.get(adjIdx);
						double adaption_effect = Math.exp( -1.0 * Math.pow(effect_radius, 2) / ( 2 * Math.pow(radiusOfInfluence, 2) ));
						cnm.addToClusterCenter(i, gridNumericalBinValues.get(effected_grid),  gridCategoricalValues.get(effected_grid), learningInfluence * adaption_effect);
					}
					
					// update the new grid for the instance
					gridAssignmentForInstances[i] = gridIndex;
				}
			}
			currIt++;
		}
		
		//TODO: printing out cluster centers for all grid cells
//		for(int j = 0; j < numGrids; j++) {
//			System.out.println(j + ">>>>" + gridNumericalBinValues.get(j).getClusterValues());
//			System.out.println(j + ">>>>" + gridCategoricalValues.get(j).getClusterValues());
//			System.out.println("");
//		}
		
		return success;
	}

	private int determineMostSimilarGridForInstance(int dataIdx) {
		int i = 0;
		
		double largestSimilarity = -1;
		int bestGrid = -1;
		for(; i < numGrids; i++) {
			double similarity = cnm.getInstanceSimilarityScoreToClusterCenter(dataIdx, gridNumericalBinValues.get(i), gridCategoricalValues.get(i));
			if(similarity > largestSimilarity) {
				bestGrid = i;
				largestSimilarity = similarity;
			}
			if(similarity == 1.0) {
				break;
			}
		}
		
		return bestGrid;
	}

	private void setValuesToNegativeOne(int[] values) {
		int i = 0;
		int size = values.length;
		for(; i < size; i++) {
			values[i] = -1;
		}
	}
	
	public void setGridSize(int numInstances, int maxSize) {
		int size = numInstances;
		if(size > maxSize) {
			size = maxSize;
		}
		double x = Math.sqrt((double) size / (6*5));
		height = (int) Math.round(2*x);
		length = (int) Math.round(3*x);
		
		this.numGrids = height*length;
		grid.setHeight(height);
		grid.setLength(length);
		grid.setNumGrids(numGrids);
	}
	
	public void fillEmptyGrids(List<ClusterCenter> list, int numGrids) {
		int i = 0;
		for(; i < numGrids; i++) {
			list.add(new ClusterCenter());
		}
	}

	public SelfOrganizingMapGrid getGrid() {
		return grid;
	}

	public void setGrid(SelfOrganizingMapGrid grid) {
		this.grid = grid;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getHeight() {
		return height;
	}

	public void setHeight(int height) {
		this.height = height;
	}

	public int getNumGrids() {
		return numGrids;
	}

	public void setNumGrids(int numGrids) {
		this.numGrids = numGrids;
	}

	public ClusterCenterNumericalMethods getCnm() {
		return cnm;
	}

	public void setCnm(ClusterCenterNumericalMethods cnm) {
		this.cnm = cnm;
	}

	public List<ClusterCenter> getGridCategoricalValues() {
		return gridCategoricalValues;
	}

	public void setGridCategoricalValues(List<ClusterCenter> gridCategoricalValues) {
		this.gridCategoricalValues = gridCategoricalValues;
	}

	public List<ClusterCenter> getGridNumericalBinValues() {
		return gridNumericalBinValues;
	}

	public void setGridNumericalBinValues(List<ClusterCenter> gridNumericalBinValues) {
		this.gridNumericalBinValues = gridNumericalBinValues;
	}

	public int getNumInstances() {
		return numInstances;
	}

	public void setNumInstances(int numInstances) {
		this.numInstances = numInstances;
	}

	public String[][] getInstanceNumberBinMatrix() {
		return instanceNumberBinMatrix;
	}

	public void setInstanceNumberBinMatrix(String[][] instanceNumberBinMatrix) {
		this.instanceNumberBinMatrix = instanceNumberBinMatrix;
	}

	public String[][] getInstanceCategoryMatrix() {
		return instanceCategoryMatrix;
	}

	public void setInstanceCategoryMatrix(String[][] instanceCategoryMatrix) {
		this.instanceCategoryMatrix = instanceCategoryMatrix;
	}

	public String[][] getInstanceNumberBinOrderingMatrix() {
		return instanceNumberBinOrderingMatrix;
	}

	public void setInstanceNumberBinOrderingMatrix(String[][] instanceNumberBinOrderingMatrix) {
		this.instanceNumberBinOrderingMatrix = instanceNumberBinOrderingMatrix;
	}

	public double[] getNumericalWeights() {
		return numericalWeights;
	}

	public void setNumericalWeights(double[] numericalWeights) {
		this.numericalWeights = numericalWeights;
	}

	public double[] getCategoricalWeights() {
		return categoricalWeights;
	}

	public void setCategoricalWeights(double[] categoricalWeights) {
		this.categoricalWeights = categoricalWeights;
	}

	public int getMaxIt() {
		return maxIt;
	}

	public void setMaxIt(int maxIt) {
		this.maxIt = maxIt;
	}

	public double getR0() {
		return r0;
	}

	public void setR0(double r0) {
		this.r0 = r0;
	}

	public double getTau() {
		return tau;
	}

	public void setTau(double tau) {
		this.tau = tau;
	}

	public int[] getGridAssignmentForInstances() {
		return gridAssignmentForInstances;
	}

	public void setGridAssignmentForInstances(int[] gridAssignmentForInstances) {
		this.gridAssignmentForInstances = gridAssignmentForInstances;
	}

	public int[] getNumInstancesInGrid() {
		return numInstancesInGrid;
	}

	public void setNumInstancesInGrid(int[] numInstancesInGrid) {
		this.numInstancesInGrid = numInstancesInGrid;
	}

	public double getL0() {
		return l0;
	}
	
	public void setL0(double l0) {
		this.l0 = l0;
	}
}
