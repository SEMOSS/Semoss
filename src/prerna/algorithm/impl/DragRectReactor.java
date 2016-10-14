package prerna.algorithm.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class DragRectReactor extends MathReactor{
	//This pkql is being created for creating clusters manually by dragging a rectangle on the plane
	//pkql should be like m:DragRectangleCluster([xcolumn,ycolumn], {Condition1:x1,Condition2:y1,Condition3:x2,Condition4:y2})
	
	@Override
	public Iterator process(){
		modExpression();
		Vector<String> columns=  (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		String[] columnArray = convertVectorToArray(columns);
		ITableDataFrame df = (ITableDataFrame)  myStore.get("G");
		Iterator itr = getTinkerData(columns, df , false);
		Map<Object,Integer> clusters = new HashMap<>();
		String script = columnArray[0];
		
		//4 points in the rectangle are A, B, C and D where AB is perpendicular to AD
		//Considering the rectangle's edges are parallel to x and y axis
		//we are passing A and C coordinates from the front end
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		double xA = Double.parseDouble(options.get("CONDITION1").toString());
		double yA = Double.parseDouble(options.get("CONDITION2").toString());
		double xC = Double.parseDouble(options.get("CONDITION3").toString());
		double yC = Double.parseDouble(options.get("CONDITION4").toString());
		
		/*double xB = xA;
		double yB = yC;
		double xD = xC;
		double yD = yA;*/
		
		while(itr.hasNext()){
			Object[] row = (Object[])itr.next();
			Double xM = (Double) row[0];
			Double yM = (Double) row[1];
			// with rectangle on coordinates ABCD where AB is perpendicular to AD a point M(xM,yM) will be inside rectangle if
						
			int cluster = 0;		
			if((xA<xM && xM<xC ) && ( yC<yM && yM<yA))
				cluster = 1;
				
			
			clusters.put(row[0], cluster);
			
		}
			
		Iterator resultItr = getTinkerData(columns, df, false);
		DragColumnIterator expItr = new DragColumnIterator(resultItr,columnArray,script,clusters);
		String nodeStr = myStore.get(whoAmI).toString();
		HashMap<String,Object> additionalInfo = new HashMap<>();
		//additionalInfo.put("Quantiles", quantileValues);
		//myStore.put("ADDITIONAL_INFO", additionalInfo);
		myStore.put(nodeStr, expItr);
		myStore.put("STATUS", STATUS.SUCCESS);
			
		return expItr;
	
	}

}
class DragColumnIterator extends ExpressionIterator{
	
	protected Map<Object,Integer> inCluster;
	
	protected DragColumnIterator() {
		
	}
	
	public DragColumnIterator(Iterator results, String [] columnsUsed, String script, Map<Object,Integer> inCluster)
	{
		this.inCluster = inCluster;
		setData(results, columnsUsed, script);
	}
		
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return (results != null && results.hasNext());
	}
	
	@Override
	public Object next() {
		Object retObject = null;
		
		if(results != null && !errored)
		{
			setOtherBindings();
			retObject = inCluster.get(otherBindings.get(columnsUsed[0]));
		}
		return retObject;
	}
}
