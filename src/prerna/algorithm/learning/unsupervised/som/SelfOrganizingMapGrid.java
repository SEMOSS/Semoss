package prerna.algorithm.learning.unsupervised.som;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelfOrganizingMapGrid {
	
	public static final String ADJACENT_CELLS_KEY = "adjacentCells";
	public static final String ADJACENT_CELLS_RADIUS_KEY = "radiusOfCells";
	
	private int length;
	private int height;
	private int numGrids;

	public static void main(String[] args) {
		SelfOrganizingMapGrid grid = new SelfOrganizingMapGrid(26, 17);
		System.out.println(grid.getAdjacentCellsInRadius(104, 13));
	}

	public SelfOrganizingMapGrid() {
		
	}
	
	public SelfOrganizingMapGrid(int length, int height) {
		this.length = length;
		this.height = height;
		this.numGrids = length*height;
	}

	public Map<String, List<Integer>> getAdjacentCellsInRadius(int cell, double radius) {
		Map<String, List<Integer>> retHash = new HashMap<String, List<Integer>>();
		List<Integer> adjacentCells = new ArrayList<Integer>();
		List<Integer> radiusForCells = new ArrayList<Integer>();
		retHash.put(ADJACENT_CELLS_KEY, adjacentCells);
		retHash.put(ADJACENT_CELLS_RADIUS_KEY, radiusForCells);
		
		int rad = (int) Math.round(radius);
		if(rad == 0) {
			return retHash;
		}

		// [x-coordinate, y-coordinate]
		int[] cellPosition = getCoordinatesOfCell(cell);

		int i = rad;
		for(; i > 0; i--) {
			// add cell to the left
			if(cellPosition[0] - i >= 0) {
				adjacentCells.add(cell-i);
				radiusForCells.add(i);
			}
			// add cell to the right
			if(cellPosition[0] + i < length) {
				adjacentCells.add(cell+i);
				radiusForCells.add(i);
			}

			// add cell to the top
			if(cell - i*length >= 0) {
				adjacentCells.add(cell-i*length);
				radiusForCells.add(i);
			}
			// add cell to the bottom
			if(cell + i*length < numGrids) {
				adjacentCells.add(cell+i*length);
				radiusForCells.add(i);
			}

			// add cell that is diagonal to the left
			if(cellPosition[0] - i >= 0) {
				// top-left
				if(cell-1 - i*length >= 0) {
					adjacentCells.add(cell-1-i*length);
					radiusForCells.add(i);
				}
				// bottom-left
				if(cell-1 + i*length < numGrids) {
					adjacentCells.add(cell-1+i*length);
					radiusForCells.add(i);
				}
			}

			// add cell that is diagonal to the right
			if(cellPosition[0] + i < length) {
				// top-left
				if(cell+1 - i*length >= 0) {
					adjacentCells.add(cell+1-i*length);
					radiusForCells.add(i);
				}
				// bottom-left
				if(cell+1 + i*length < numGrids) {
					adjacentCells.add(cell+1+i*length);
					radiusForCells.add(i);
				}
			}

			// add annoying cells that are not directly diagonal
			if(i > 1) {
				// check and add all values moving up the grid
				for(int j = 1; j <= i; j++) {
					if(cellPosition[0] - i >= 0 && cell - i - j*length >= 0) {
						adjacentCells.add(cell - i - j*length);
						radiusForCells.add(i);
					} else {
						break;
					}
				}
				// check and add all values moving down the grid
				for(int j = 1; j <= i; j++) {
					if(cellPosition[0] - i >= 0 && cell - i + j*length < numGrids) {
						adjacentCells.add(cell - i + j*length);
						radiusForCells.add(i);
					} else {
						break;
					}
				}
				// check and add all values moving up the grid
				for(int j = 1; j <= i; j++) {
					if(cellPosition[0] + i < length && cell + i - j*length >= 0) {
						adjacentCells.add(cell + i - j*length);
						radiusForCells.add(i);
					} else {
						break;
					}
				}
				// check and add all values moving down the grid
				for(int j = 1; j <= i; j++) {
					if(cellPosition[0] + i < length && cell + i + j*length <= numGrids) {
						adjacentCells.add(cell + i + j*length);
						radiusForCells.add(i);
					} else {
						break;
					}
				}
			}
		}

		return retHash;
	}

	// return array of [x-coordinate, y-coordinate]
	private int[] getCoordinatesOfCell(int cell) {
		int x_position = cell % length;
		int y_position = cell - length * (x_position-1);
		return new int[]{x_position, y_position};
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

}
