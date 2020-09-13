package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

public class KMeansModel {
	
	private List<DataPoint> points;
	private List<Cluster> clusters ;
	
	public KMeansModel(Iterator itr,List<Integer> clusteringAttributeIndices, int numIterations, int maxClusters){
		points = new ArrayList<>();
		while(itr.hasNext()){
			Object[] row = (Object[]) itr.next();
			int numDims = clusteringAttributeIndices.size();
			double[] dim  = new double[numDims];
			for(int i=0; i<numDims; i++){
				dim[i] = (double)row[clusteringAttributeIndices.get(i)];
			}
			DataPoint point = new DataPoint(row,dim);
			points.add(point);				
		}
		Collections.sort(points);
		clusters = InitClusters(maxClusters);
		Expectation();
		while(numIterations > 0){
			Maximization();
			Expectation();
			numIterations--;
		}
	}
	
	List<Cluster> InitClusters(int maxClusters){
		List<double[]> pointData = new ArrayList<>();
		for(DataPoint point : points)
			pointData.add(point.dimensions);
		/*KMeansNumClusters kmeansNumClusters = new KMeansNumClusters();
		int numClusters = kmeansNumClusters.calcNumClusters(pointData);
		if (maxClusters > 0)
			numClusters = Math.min(numClusters, maxClusters);*/
		int numClusters = maxClusters;
		KMeansInit kmeansInit = new KMeansInit(numClusters);
		List<double[]>centres = kmeansInit.cluster(pointData);
		List<Cluster> clusters = new ArrayList<Cluster>(numClusters);
		for(double[] centrePos : centres){
			DataPoint centre = new DataPoint(null, centrePos);
			Cluster cluster = new Cluster(centre);
			clusters.add(cluster);
		}
		return clusters;
	}
	
	public Map<List<Object>,Integer> clusterResult(){
		Map<List<Object>, Integer> result = new HashMap<List<Object>,Integer>();
		TreeSet<Cluster> clusters2 = new  TreeSet<>();
		for(DataPoint p : points){
			clusters2.add(p.cluster);
		}
		
		for(DataPoint p : points){
			result.put(p.id, clusters2.headSet(p.cluster).size());
		}
		return result;
	}
	
	public double getSSE(){
		double SSE = 0;
		for(DataPoint p : points){
			SSE += DataPoint.EucledianDistance(p, p.cluster.centre);
		}
		return SSE;
	}
	
	public Map<String,Object> getMetaData(){
		TreeSet<Cluster> clusters2 = new  TreeSet<>();
		for(DataPoint p : points){
			clusters2.add(p.cluster);
		}
		
		Map<String, Object> clustersMetaData = new HashMap<String,Object>();
		for(Cluster c: clusters){
			HashMap<String,Object> clusterData= new HashMap<>();
			clusterData.put("numPoints", 0);
			String key = "Autocalculated " + clusters2.headSet(c).size();
			clustersMetaData.put(key, clusterData);
		}
		
		for(DataPoint p : points){
			String key = "Autocalculated " + clusters2.headSet(p.cluster).size();
			HashMap<String,Object> clusterData = (HashMap<String,Object>)clustersMetaData.get(key);
			clusterData.put("numPoints",(Integer)clusterData.get("numPoints") + 1);
		}
		
//		for(Cluster c : clusters){
//			StringBuilder sb = new StringBuilder();
//			for(double d : c.centre.dimensions){
//				sb.append(String.format("%.2f,", d));
//			}
//			sb.replace(sb.length() - 1, sb.length(), "");
//		clusterCentres.put(String.valueOf(clusters.indexOf(c)), sb.toString());
//		}
		return clustersMetaData;
	}
	
	void Expectation(){
		for(DataPoint p : points){
			p.cluster = null;
			double minDist = Double.MAX_VALUE;
			for(Cluster c : clusters){
				double distance = DataPoint.EucledianDistance(c.centre, p);
				if(distance < minDist){
					minDist = distance;
					p.cluster = c;
				}
			}
		}
	}
	
	void Maximization(){
		double[][] sumDataPointDimPerCluster = new double[clusters.size()][points.get(0).dimensions.length];
		int[] numDataPointsPerCluster = new int[clusters.size()];
		for(DataPoint point : points){
			for(int dim=0; dim<point.dimensions.length; dim++){
				sumDataPointDimPerCluster[clusters.indexOf(point.cluster)][dim] += point.dimensions[dim];
			}
			numDataPointsPerCluster[clusters.indexOf(point.cluster)]++;
		}
		for(Cluster cluster : clusters){
			int index = clusters.indexOf(cluster);
			double[] newCentreDim = new double[sumDataPointDimPerCluster[index].length];
			for(int dim=0;dim<newCentreDim.length;dim++){
				newCentreDim[dim] = sumDataPointDimPerCluster[index][dim]/numDataPointsPerCluster[index];
			}
			DataPoint newCentre = new DataPoint(null,newCentreDim);
			cluster.centre = newCentre;
		}
	}

}

class DataPoint implements Comparable<DataPoint>,Comparator<DataPoint> {
	final List<Object> id;
	final double[] dimensions;
	Cluster cluster;
	
	public DataPoint(Object[] id, double[] dimensions){
		this.id = (id != null)? Arrays.asList(id) : null;
		this.dimensions = dimensions;
		this.cluster = null;
	}
	
	public static double EucledianDistance(DataPoint p1, DataPoint p2){
		double sqDist = 0.0;
		for(int dim=0; dim<p1.dimensions.length;dim++){
			sqDist += Math.pow((p1.dimensions[dim] - p2.dimensions[dim]),2);
		}
		return Math.sqrt(sqDist);
	}

	@Override
	public int compare(DataPoint arg0, DataPoint arg1) {
		// TODO Auto-generated method stub
		if(arg0 == null)
			return arg1 == null? 0 : 1;
		if (arg1 == null)
			return -1;
		return arg0.compareTo(arg1);
	}

	@Override
	public int compareTo(DataPoint arg0) {
		return id.get(0).toString().compareTo(arg0.id.get(0).toString());
	}
}

class Cluster implements Comparable<Cluster>,Comparator<Cluster>{
	
	DataPoint centre;
	
	public Cluster(DataPoint centre){
		this.centre = centre;
	}

	@Override
	public int compare(Cluster o1, Cluster o2) {
		// TODO Auto-generated method stub
		if(o1 == null)
			return o2 == null? 0 : 1;
		if (o2 == null)
			return -1;
		return o1.compareTo(o2);
	}

	@Override
	public int compareTo(Cluster o) {
		// TODO Auto-generated method stub
		double myDist = 0;
		for(double dim : centre.dimensions){
			myDist += Math.pow(dim, 2);
		}
		myDist = Math.sqrt(myDist);
		
		double otherDist = 0;
		for(double dim : o.centre.dimensions){
			otherDist += Math.pow(dim, 2);
		}
		otherDist = Math.sqrt(otherDist);
		
		if(myDist > otherDist)
			return 1;
		else if (myDist == otherDist)
			return 0;
		else
			return -1;
	}
}