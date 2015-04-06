package prerna.algorithm.learning.unsupervised.som;

import java.util.ArrayList;
import java.util.List;

public class SelfOrganizingMapGridViewer extends SelfOrganizingMapGrid{

	private SelfOrganizingMapGridViewer() {
		super();
	}
	
	public static double[][] getGridCoordinates(int length, int height, int[] numInstancesInGrid) {
		double[][] coordinates = new double[height+1][length+1];
		
		int currHeight = 0;
		int currLength = 0;
		int currCell = 0;
		
		for(; currHeight < height; currHeight++) {
			currLength = 0;
			double[] newRow = new double[length+1];
			for(; currLength < length; currLength++) {
				List<Integer> adjacentCells = new ArrayList<Integer>();
				// add currentCell to list
				adjacentCells.add(currCell);
				// add surrounding cells for border point
				getLeftCells(currLength, 1, currCell, adjacentCells, new ArrayList<Integer>());
				getDiagonalTopLeftCells(currLength, length, 1, currCell, adjacentCells, new ArrayList<Integer>());
				getTopCells(currLength, length, 1, currCell, adjacentCells,  new ArrayList<Integer>());
				double avgCount = getAverageCount(adjacentCells, numInstancesInGrid);
				newRow[currLength] = avgCount;
				currCell++;
				// special processing for last column
				if(currLength == length - 1) {
					currCell--;
					adjacentCells = new ArrayList<Integer>();
					// add currentCell to list
					adjacentCells.add(currCell);
					// add surrounding cells for border point
					getTopCells(currLength, length, 1, currCell, adjacentCells,  new ArrayList<Integer>());
					avgCount = getAverageCount(adjacentCells, numInstancesInGrid);
					newRow[currLength+1] = avgCount;
					currCell++;
				}
			}
			coordinates[currHeight] = newRow;
			// special processing for last row
			if(currHeight == height - 1) {
				currCell = currCell - length;
				currHeight++;
				double[] lastRow = new double[length+1];
				currLength = 0;
				for(; currLength < length; currLength++) {
					List<Integer> adjacentCells = new ArrayList<Integer>();
					// add currentCell to list
					adjacentCells.add(currCell);
					// add surrounding cells for border point
					getLeftCells(currLength, 1, currCell, adjacentCells, new ArrayList<Integer>());
					double avgCount = getAverageCount(adjacentCells, numInstancesInGrid);
					lastRow[currLength] = avgCount;
					currCell++;
					// special processing for last column
					if(currLength == length - 1) {
						currCell--;
						adjacentCells = new ArrayList<Integer>();
						// add currentCell to list
						adjacentCells.add(currCell);
						// add surrounding cells for border point
						// no surrounding cells for last point
						avgCount = getAverageCount(adjacentCells, numInstancesInGrid);
						lastRow[currLength+1] = avgCount;
						currCell++;
					}
				}
				coordinates[currHeight] = lastRow;
			}
		}
		return coordinates;
	}
	
	private static double getAverageCount(List<Integer> adjacentCells, int[] numInstancesInGrid) {
		if(adjacentCells == null || adjacentCells.isEmpty()) {
			throw new IllegalArgumentException("Cannot calculate an average if no instances are found");
		}
		
		double avgCount = 0;
		
		int i = 0;
		int size = adjacentCells.size();
		for(; i < size; i++) {
			avgCount += numInstancesInGrid[adjacentCells.get(i)];
		}
		
		return avgCount / size;
	}
}
