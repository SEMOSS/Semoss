package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.List;

public class KMeansNumClusters {
	private int numClusters = 1;
	
	public int calcNumClusters(List<double[]> dataPoints){
		SplitAndAnalyze(dataPoints, getSplitDetails(dataPoints));
		return numClusters;
	}
	
	void SplitAndAnalyze(List<double[]> dataPoints, SplitDetails splitDetails){
		if(dataPoints.size() < 2)
		{
			numClusters+= dataPoints.size();
			return;
		}
		
		List<double[]> bound1 = new ArrayList<double[]>();
		List<double[]> bound2 = new ArrayList<double[]>();
		double[] bound1Centre = {0.0,0.0}, bound2Centre = {0.0,0.0};
		for(double[] point: dataPoints){
			if(point[splitDetails.splitAttribute] < splitDetails.centre[splitDetails.splitAttribute]){
				bound1.add(point);
				bound1Centre[0] += point[0];
				bound1Centre[1] += point[1];
			}else{
				bound2.add(point);
				bound2Centre[0] += point[0];
				bound2Centre[1] += point[1];
			}
		}
		bound1Centre[0] = bound1Centre[0]/(1.0 * bound1.size());
		bound1Centre[1] = bound1Centre[1]/(1.0 * bound1.size());

		bound2Centre[0] = bound2Centre[0]/(1.0 * bound2.size());
		bound2Centre[1] = bound2Centre[1]/(1.0 * bound2.size());
		
		//double centreDist = Math.sqrt(Math.pow(bound1Centre[0] - bound2Centre[0], 2) + Math.pow(bound1Centre[1] - bound2Centre[1], 2));
		double bound1radius = 0.0, bound2radius = 0.0;
		/*for(double[] point : bound1)
			bound1radius += Math.sqrt(Math.pow(bound1Centre[0] - point[0], 2) + Math.pow(bound1Centre[1] - point[1], 2))/(1.0 * bound1.size());
		for(double[] point : bound2)
			bound2radius += Math.sqrt(Math.pow(bound2Centre[0] - point[0], 2) + Math.pow(bound2Centre[1] - point[1], 2))/(1.0 * bound1.size());*/
		
		double bound1Scatter = 0.0, bound2Scatter = 0.0;
		int points = 0;
		for(int i=0; i<bound1.size() - 1;i++){
			for(int j=i+1; j<bound1.size();j++){
				bound1radius += Math.sqrt(Math.pow(bound1.get(i)[0] - bound1.get(j)[0], 2) + Math.pow(bound1.get(i)[1] - bound1.get(j)[1], 2));
				points++;
			}
			bound1Scatter += 1.0/Math.sqrt(Math.pow(bound1.get(i)[0] - bound1Centre[0], 2) + Math.pow(bound1.get(i)[1] - bound1Centre[1], 2));
		}
		bound1radius = (points != 0) ? bound1radius/(1.0 * points) : 0;
		points = 0;
		for(int i=0; i<bound2.size() - 1;i++){
			for(int j=i+1; j<bound2.size();j++){
				bound2radius += Math.sqrt(Math.pow(bound2.get(i)[0] - bound2.get(j)[0], 2) + Math.pow(bound2.get(i)[1] - bound2.get(j)[1], 2));
				points++;
			}
			bound2Scatter += 1.0/Math.sqrt(Math.pow(bound2.get(i)[0] - bound2Centre[0], 2) + Math.pow(bound2.get(i)[1] - bound2Centre[1], 2));
		}
		bound2radius = (points != 0) ? bound2radius/(1.0 * points) : 0;
		
		double radius = Math.max(bound1radius, bound2radius);
		double centreDist = Math.sqrt(Math.pow(bound1Centre[0] - bound2Centre[0], 2) + Math.pow(bound1Centre[1] - bound2Centre[1], 2));
		double scatterRatio = Math.min(bound1Scatter, bound2Scatter)/Math.max(bound1Scatter, bound2Scatter);
		
		//double maxRadius = Math.max(bound1radius, bound2radius);
		//double ratio = Math.min(centreDist, maxRadius)/Math.max(centreDist, maxRadius);
		
		/*double centreBound1Radius = Math.sqrt(Math.pow(bound1Centre[0] - splitDetails.centre[0], 2) + Math.pow(bound1Centre[1] - splitDetails.centre[1], 2)) + bound1radius;
		double centreBound2Radius = Math.sqrt(Math.pow(bound2Centre[0] - splitDetails.centre[0], 2) + Math.pow(bound2Centre[1] - splitDetails.centre[1], 2)) + bound2radius;
		
		double radius = Math.max(centreBound1Radius, centreBound2Radius);
		double centreDist = Math.sqrt(Math.pow(bound1Centre[0] - bound2Centre[0], 2) + Math.pow(bound1Centre[1] - bound2Centre[1], 2));
		double ratio = Math.min(radius, centreDist)/Math.max(radius, centreDist);*/
		
		System.out.println("Bound 1: " + bound1Centre[0] + " " + bound1Centre[1]);
		System.out.println("Bound 2: " + bound2Centre[0] + " " + bound2Centre[1]);
		System.out.println("Bound 1 average distance: " + bound1radius);
		System.out.println("Bound 2 average distance: " + bound2radius);
		System.out.println("Bounds Inter-Centre Distance " + centreDist);
		System.out.println("Bounds ScatterRatio " + scatterRatio);
		if(radius/centreDist > 0.8 && scatterRatio > 0.01){
			System.out.println("No need for split ");
			return;
		}
		else{
			System.out.println("Good split ");
			numClusters++;
			SplitAndAnalyze(bound1, getSplitDetails(bound1));
			SplitAndAnalyze(bound2, getSplitDetails(bound2));
		}
	}
	
	SplitDetails getSplitDetails(List<double[]> dataPoints){
		double[] centre = {0.0,0.0};
		double minX = Double.MAX_VALUE, minY = Double.MAX_VALUE, maxX = Double.MIN_VALUE, maxY = Double.MIN_VALUE;
		for(double[] point : dataPoints){
			minX = Math.min(minX, point[0]);
			minY = Math.min(minY, point[1]);
			maxX = Math.max(maxX, point[0]);
			maxY = Math.max(maxY, point[1]);
			centre[0] += point[0]/(1.0 * dataPoints.size());
			centre[1] += point[1]/(1.0 * dataPoints.size());
		}
		double rangeX = maxX - minX;
		double rangeY = maxY - minY;
		
		// Function for evaluating distribution across both attributes.
		double splitX = 0.0, splitY = 0.0;
		for(double[] point : dataPoints){
			splitX += rangeX/Math.abs(centre[0] - point[0]);
			splitY += rangeY/Math.abs(centre[1] - point[1]);
		}
		SplitDetails splitDetails = new SplitDetails();
		splitDetails.centre = centre;
		if(splitX < splitY)
			splitDetails.splitAttribute = 0;
		else
			splitDetails.splitAttribute = 1;
		return splitDetails;
	}
}

class SplitDetails{
	int splitAttribute = -1;
	double[] centre = {0.0,0.0};
}