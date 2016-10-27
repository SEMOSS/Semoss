package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import prerna.sablecc.PKQLEnum;

public class VLookupReactor extends BaseReducerReactor {
	
	@Override
	public Object reduce() {
		String output=null;
		String inputS=null;
		double tp;
		double tp1;
		double tp2=0;
		int rfound=0;
		int iInit=0;
		int sValPos=1;
	    ArrayList<String> sArray  = new ArrayList<String>();
	    ArrayList<String> rArray  = new ArrayList<String>();
		//Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MATH_PARAM);
		int valPosition = Integer.parseInt(options.get("CONDITION1") + "");
		int colPosition = Integer.parseInt(options.get("CONDITION2") + "");		
		boolean exact_approx =Boolean.valueOf(options.get("CONDITION3") + "");
		int startRow = Integer.parseInt(options.get("CONDITION4") + "");	
		int endRow = Integer.parseInt(options.get("CONDITION5") + "");	
		while(inputIterator.hasNext() && !errored)
		{
			ArrayList dec = (ArrayList)getNextValue();
			//need to check index
			if(sValPos==valPosition){
			inputS =dec.get(0).toString();
			}
			sValPos +=1;
			sArray.add(dec.get(1).toString());
			rArray.add(dec.get(colPosition).toString());			
			
		}
		sValPos=1;
		int length=sArray.size();
		for (int i = 0; ((i < length)&&rfound==0) ; i++)
		{
			System.out.println(sArray.get(i).toString());
			System.out.println(rArray.get(i).toString());
			if (i>=(startRow-1) && i<=(endRow-1)  )
			{
					if((inputS).equals(sArray.get(i).toString()) && exact_approx==false)
						{
							output=rArray.get(i).toString();
							rfound=1;
							System.out.println(output);
							
						}
				
					else if(exact_approx==true)
						{   
							tp=Double.parseDouble(sArray.get(i).toString()) ;
							tp1=Math.abs(tp-Double.parseDouble(inputS));
														
							if (iInit==0)
							{output=rArray.get(i).toString();;
							iInit=1;
							tp2=tp1 ;								
							}
							else if (tp1<tp2)
							{tp2=tp1;
							output=rArray.get(i).toString();
							}
					    }
			}
			
			
		}
	
		if(output instanceof String) {
			output = "\"" + output + "\"";
		}
		
		System.out.println(output);
		return output;
	}

	@Override
	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys,
			Vector<String> processedColumns, String[] columnsArray, Iterator it) {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public Map<String, Object> getColumnDataMap() {
		// this operation cannot be visualized in 
		// viz panel
		return null;
	}
}
//SearchValue,resultposition,T/F,StartRow,EndRow
//m:VLookup([c:CYREVENUE,c:PYREVENUE,c:CUSTOMER],{"CONDITION1":5,"CONDITION2":3,"CONDITION3":"FALSE","CONDITION4":5,"CONDITION5":10});
//m: VLookup ( [ c: PYREVENUE , c: CYREVENUE ] , { "CONDITION1" : 12 , "CONDITION2" : 2 , "CONDITION3" : "TRUE" } ) ; 