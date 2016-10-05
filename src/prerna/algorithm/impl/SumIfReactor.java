package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc.PKQLEnum;

public class SumIfReactor extends BaseReducerReactor {

	@Override
	public Object reduce() {
		double sum = 0;
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		String condition = options.get("CONDITION1") + "";
		double value =0;
		if(condition.contains("<=")){	
			value = Double.parseDouble( condition.trim().substring(condition.indexOf("<=")+2));					
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(Double.parseDouble( dec.get(0).toString().trim())<= value) {
					sum += Double.parseDouble( dec.get(0).toString().trim()) ;
				}
			}
		}
		else if(condition.contains(">=")){		
			value = Double.parseDouble( condition.trim().substring(condition.indexOf(">=")+2));
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(Double.parseDouble( dec.get(0).toString().trim())>= value) {
					sum += Double.parseDouble( dec.get(0).toString().trim()) ;
				}
			}
		}
		else if(condition.contains("<")){		
			value = Double.parseDouble( condition.trim().substring(condition.indexOf("<")+1));
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(Double.parseDouble( dec.get(0).toString().trim())< value) {
					sum += Double.parseDouble( dec.get(0).toString().trim()) ;
				}
			}
		}
		else if(condition.contains(">")){		
			value = Double.parseDouble( condition.trim().substring(condition.indexOf(">")+1));
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(Double.parseDouble( dec.get(0).toString().trim())> value) {
					sum += Double.parseDouble( dec.get(0).toString().trim()) ;
				}
			}
		}
		else if(condition.contains("<>")){		
			value = Double.parseDouble( condition.trim().substring(condition.indexOf("<>"))+2);
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(Double.parseDouble( dec.get(0).toString().trim()) != value) {
					sum += Double.parseDouble( dec.get(0).toString().trim()) ;
				}
			}
		}
		else {	
			try{
			value = Double.parseDouble( condition.trim());
			while(inputIterator.hasNext()) {
				ArrayList dec = (ArrayList)getNextValue();
				if(Double.parseDouble( dec.get(0).toString().trim()) == value) {
					sum += Double.parseDouble( dec.get(0).toString().trim()) ;
				}
			}}
			catch(Exception e)
			{	
				String val= condition.trim();
				while(inputIterator.hasNext()) {
					ArrayList dec = (ArrayList)getNextValue();
					if(dec.get(0).toString().trim().equals( val)) {
						 sum ++ ;
					}
				}
				
			}
		}
		
		System.out.println(sum);
		return sum;
	}
	
	@Override
	public HashMap<HashMap<Object,Object>,Object> reduceGroupBy(Vector<String> groupBys, Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		HashMap<HashMap<Object,Object>, Object> groupByHash = new HashMap<HashMap<Object,Object>,Object>();
		
		//TODO
		return groupByHash;
	}
	
}