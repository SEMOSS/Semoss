package prerna.algorithm.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc.PKQLEnum;

public class VLookupReactor extends BaseReducerReactor {

	private static final Logger logger = LogManager.getLogger(VLookupReactor.class);

	public VLookupReactor() {
		setMathRoutine("VLookUp");
	}
	
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
	    ArrayList<String> sArray  = new ArrayList<>();
	    ArrayList<String> rArray  = new ArrayList<>();
		//Vector<String> columns = (Vector <String>) myStore.get(PKQLEnum.COL_DEF);
		Map<String, Object> options = (Map<String, Object>) myStore.get(PKQLEnum.MAP_OBJ);
		int valPosition = Integer.parseInt(options.get("CONDITION1") + "");
		int colPosition = Integer.parseInt(options.get("CONDITION2") + "");		
		boolean exactApprox =Boolean.valueOf(options.get("CONDITION3") + "");
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

		if (inputS == null) {
			throw new NullPointerException("inputS cannot be null here.");
		}

		sValPos = 1;
		int length=sArray.size();
		for (int i = 0; ((i < length) && rfound == 0); i++) {
			logger.info(sArray.get(i));
			logger.info(rArray.get(i));
			if (i >= (startRow - 1) && i <= (endRow - 1)) {
				if ((inputS).equals(sArray.get(i)) && !exactApprox) {
					output = rArray.get(i);
					rfound = 1;
					logger.info(output);

				}

				else if (exactApprox) {
					tp = Double.parseDouble(sArray.get(i));
					tp1 = Math.abs(tp - Double.parseDouble(inputS));

					if (iInit == 0) {
						output = rArray.get(i);
						iInit = 1;
						tp2 = tp1;
					} else if (tp1 < tp2) {
						tp2 = tp1;
						output = rArray.get(i);
					}
				}
			}

		}
	
		if(output instanceof String) {
			output = "\"" + output + "\"";
		}

		logger.info(output);

		return output;
	}

	@Override
	public HashMap<HashMap<Object, Object>, Object> reduceGroupBy(Vector<String> groupBys,
			Vector<String> processedColumns, String[] columnsArray, Iterator it) {
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