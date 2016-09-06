package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;

public class MathParamReactor extends AbstractReactor{

	public final String PARAM_OBJ_KEY = "mathParamObject";
	
	public MathParamReactor() {
		String [] thisReacts = {PKQLEnum.EXPR_TERM, PKQLEnum.DECIMAL, PKQLEnum.NUMBER, PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.MATH_PARAM;
	}
	
	@Override
	public Iterator process() {
		// values are stored in an array
		// the even numbered i-th entry is the key for the (i+1)th entry
		Vector<Object> values = (Vector<Object>) myStore.get(PKQLEnum.WORD_OR_NUM);
		
		if(values != null) {
			if(values.size() % 2 != 0) {
				System.err.println("Map object is not valid...");
			}
			
			// loop through and grab each key at ith position starting at 0
			// set the value of each key to the (i+1)th position
			Map<String, Object> params = new Hashtable<String, Object>();
			for(int i = 0; i < values.size(); i=i+2) {
				params.put((values.get(i) + "").toUpperCase(), values.get(i+1));
			}
			
			myStore.put(PKQLEnum.MATH_PARAM, params);		
		}

		return null;
	}
}
