package prerna.algorithm.learning.unsupervised.som;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SelfOrganizingMapGrid {

	public static final String ADJACENT_CELLS_KEY = "adjacentCells";
	public static final String ADJACENT_CELLS_RADIUS_KEY = "radiusOfCells";

	private int length;
	private int width;
	private int numGrids;

	public static void main(String[] args) {
		SelfOrganizingMapGrid grid = new SelfOrganizingMapGrid(26, 17);
		System.out.println(grid.getAdjacentCellsInRadius(104, 13));
		
		System.out.println(Arrays.toString(SelfOrganizingMapGrid.getCoordinatesOfCell(354, 26)));
	}

	public SelfOrganizingMapGrid() {

	}

	public SelfOrganizingMapGrid(int length, int width) {
		this.length = length;
		this.width = width;
		this.numGrids = length*width;
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
		int[] cellPosition = getCoordinatesOfCell(cell, length);

		int i = rad;
		for(; i > 0; i--) {
			// add cell to the left
			getLeftCells(cellPosition[0], i, cell, adjacentCells, radiusForCells);
			// add cell to the right
			getRightCells(cellPosition[0], length, i, cell, adjacentCells, radiusForCells);
			// add cell to the top
			getTopCells(cellPosition[0], length, i, cell, adjacentCells, radiusForCells);
			// add cell to the bottom
			getBottomCells(cellPosition[0], length, numGrids, i, cell, adjacentCells, radiusForCells);

			// add cell that is diagonal to the left
			getDiagonalBottomLeftCells(cellPosition[0], length, numGrids, i, cell, adjacentCells, radiusForCells);
			getDiagonalTopLeftCells(cellPosition[0], length, i, cell, adjacentCells, radiusForCells);
			// add cell that is diagonal to the right
			getDiagonalBottomRightCells(cellPosition[0], length, numGrids, i, cell, adjacentCells, radiusForCells);
			getDiagonalTopRightCells(cellPosition[0], length, i, cell, adjacentCells, radiusForCells);

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

	protected static void getLeftCells(int x_coordinate, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(x_coordinate - radius >= 0) {
			adjacentCells.add(cell-radius);
			radiusForCells.add(radius);
		}
	}

	protected static void getRightCells(int x_coordinate, int length, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(x_coordinate + radius < length) {
			adjacentCells.add(cell+radius);
			radiusForCells.add(radius);
		}
	}

	protected static void getTopCells(int x_coordinate, int length, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(cell - radius*length >= 0) {
			adjacentCells.add(cell-radius*length);
			radiusForCells.add(radius);
		}
	}

	protected static void getBottomCells(int x_coordinate, int length, int numGrids, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(cell + radius*length < numGrids) {
			adjacentCells.add(cell+radius*length);
			radiusForCells.add(radius);
		}
	}

	protected static void getDiagonalTopLeftCells(int x_coordinate, int length, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(x_coordinate + radius < length) {
			// top-left
			if(cell+1 - radius*length >= 0) {
				adjacentCells.add(cell+1-radius*length);
				radiusForCells.add(radius);
			}
		}
	}

	protected static void getDiagonalBottomLeftCells(int x_coordinate, int length, int numGrids, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(x_coordinate + radius < length) {
			// bottom-left
			if(cell-1 + radius*length < numGrids) {
				adjacentCells.add(cell-1+radius*length);
				radiusForCells.add(radius);
			}
		}
	}

	protected static void getDiagonalTopRightCells(int x_coordinate, int length, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(x_coordinate + radius < length) {
			// top-right
			if(cell+1 - radius*length >= 0) {
				adjacentCells.add(cell+1-radius*length);
				radiusForCells.add(radius);
			}
		}
	}

	protected static void getDiagonalBottomRightCells(int x_coordinate, int length, int numGrids, int radius, int cell, List<Integer> adjacentCells, List<Integer> radiusForCells) {
		if(x_coordinate + radius < length) {
			// bottom-right
			if(cell+1 + radius*length < numGrids) {
				adjacentCells.add(cell+1+radius*length);
				radiusForCells.add(radius);
			}
		}
	}

	// return array of [x-coordinate, y-coordinate]
	public static int[] getCoordinatesOfCell(int cell, int length) {
		int x_position = cell % length;
		int y_position = (int) Math.floor( (double) cell/length);
		return new int[]{x_position, y_position};
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	public int getNumGrids() {
		return numGrids;
	}

	public void setNumGrids(int numGrids) {
		this.numGrids = numGrids;
	}

}
