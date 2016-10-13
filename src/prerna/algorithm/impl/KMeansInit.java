package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

// This clustering algorithm uses a density based approach to find the intial cluster centres and then proceeds with K-Means
// for actual clustering and converging to the actual result.

//To do : If input attributes are more than 2, use LSA to reduce it to a 2-d space.

public class KMeansInit {
	
	private int numClusters = 0;
	private int m, n;
	
	public KMeansInit(int numClusters){
		this.numClusters = numClusters;
		m = Math.round((float)Math.sqrt(numClusters));
		n = 1;
		while(true){
			if(numClusters - m *n <= 0)
				break;
			n++;
		}
		System.out.println("Grid : " + m + " x " + n);
	}
	
	// To do : Use LSI to convert to 2-d space if number of dimensions is more than 2.
	public List<double[]> cluster(List<double[]> data){
		double minX = Double.MAX_VALUE, minY = Double.MAX_VALUE;
		double maxX = Double.MIN_VALUE, maxY = Double.MIN_VALUE;
		
		// Find range of data to setup grid.
		for(double[] point : data){
			minX = Math.min(minX, point[0]);
			minY = Math.min(minY, point[1]);
			maxX = Math.max(maxX, point[0]);
			maxY = Math.max(maxY, point[1]);
		}
		
		double stepX = (maxX - minX)/m, stepY = (maxY - minY)/n;
		
		int[] gridCount = new int[m*n];
		
		// Initialize grid data. Will be used to calculate initial cluster centres.
		List<double[]> gridData = new ArrayList<double[]>(m * n);
		for(int i=0; i<m*n; i++){
			double[] temp = {0,0};
			gridData.add(temp);
		}
		
		// Aggregate data points per cluster
		for(double[] point : data){
			int xSlot = (int) Math.round((float)(point[0] - minX)/stepX);
			xSlot = (xSlot >= m) ? m - 1 : ((xSlot < 0) ? 0 : xSlot);
			int ySlot = (int) Math.round((float)(point[1] - minY)/stepY);
			ySlot = (ySlot >= n) ? n - 1 : ((ySlot < 0) ? 0 : ySlot);
			gridCount[xSlot * n + ySlot] += 1;
			double[] newData = gridData.get(xSlot * n + ySlot);
			newData[0] += point[0];
			newData[1] += point[1];
			gridData.set(xSlot * n + ySlot, newData);
		}
		
		// Calculate grid centres
		for(int i=0; i<m;i++){
			for(int j=0; j<n; j++){
				double[] gridCentre = gridData.get(i* n + j);
				gridCentre[0] /= gridCount[i*n + j];
				gridCentre[1] /= gridCount[i*n + j];
				gridData.set(i*n + j, gridCentre);
			}
		}
		
		Map<Integer,Integer> gridCountMap = new HashMap<>();
		for(int i=0; i<gridCount.length; i++){
			gridCountMap.put(i, gridCount[i]);
		}
		gridCountMap = sortByValue(gridCountMap);
		List<Integer> keys = new ArrayList<>(gridCountMap.keySet());
		
		int iterations = this.numClusters;
		int index = 0;
		
		List<double[]> result = new ArrayList<>();
		while(iterations != 0){
			double[] centre = gridData.get(keys.get(index));
			result.add(centre);
			index = (index < numClusters/2)? numClusters - index - 1 : numClusters - index;
			iterations--;
		}
		
		return result;
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map )
	{
		List<Map.Entry<K, V>> list = new LinkedList<>( map.entrySet() );
		Collections.sort( list, new Comparator<Map.Entry<K, V>>()
		{
		    @Override
		    public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 )
		    {
		        return ( o1.getValue() ).compareTo( o2.getValue() );
		    }
		} );
		
		Map<K, V> result = new LinkedHashMap<>();
		for (Map.Entry<K, V> entry : list)
		{
		    result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}
}