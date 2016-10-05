package prerna.algorithm.impl;

import java.util.Iterator;
import java.util.Vector;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.ExpressionIterator;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

public class IFReactor extends MathReactor { 
    /*I am creating this pkql for handling the logical operator IF
     * for using it col.add ( c: IF , m: IF ( [ ( ( c: CYREVENUE < 10000 ) && ( c: PYREVENUE = c: CYREVENUE ) ) ] ) ) ; 
     * where the last condition should be the action that we have to take on 
     * success or failure evaluation of the condition within if statement
     * && and = should is hardcoded to identify the action taken according to condition
     * @see Translation.java
     */
    @Override
    public Iterator process() {
          modExpression();
          Vector <String> columns = (Vector <String>)myStore.get(PKQLEnum.COL_DEF);
          String[] columnsArray = convertVectorToArray(columns);
          
          Iterator iterator = getTinkerData(columns, (ITableDataFrame)myStore.get("G"), false);

          String nodeStr = myStore.get(whoAmI).toString();
          String expression = myStore.get("MATH_FUN").toString().replace("c:", "").replace("[ (", "[ ").replace(") ]", " ]").trim();
          
          String positiveCase = myStore.get("LEFT_VAL").toString().replace("c:", "").trim();
          String negativeCase = myStore.get("RIGHT_VAL").toString().replace("c:", "").trim();
          
                             
          //String finalScript1 = "[ (CYREVENUE >10000) ?  "+positiveCase+" : "+negativeCase+"]";
          
          String toreplace =  positiveCase + "=" + negativeCase ;
        	  String action =  positiveCase + " : " + negativeCase ;
        	  String test = expression.replace(" ", "");
        	  String condition = test.replace("&&(" + toreplace + ")]", " ? ") + action + "]";
          
	          ExpressionIterator expIt = new ExpressionIterator(iterator, columnsArray,condition );
	          myStore.put(nodeStr, expIt);
	          myStore.put("STATUS",STATUS.SUCCESS);
	          
	          return expIt;
          
    }
    
}