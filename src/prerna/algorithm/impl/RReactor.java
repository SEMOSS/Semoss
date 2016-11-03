package prerna.algorithm.impl;

import java.sql.SQLException;
import java.util.Iterator;

import org.rosuda.REngine.Rserve.RserveException;

import prerna.ds.h2.H2Frame;
import prerna.engine.impl.r.RRunner;
import prerna.sablecc.MathReactor;
import prerna.sablecc.PKQLEnum;
import prerna.sablecc.PKQLRunner.STATUS;

/**
 * Reacts to PKQL commands invoking R scripts, ex. {@code m:R([<code>script<code>}]);
 * @author kepark
 *
 */
public class RReactor extends MathReactor{
	
	@Override
	public Iterator process() {
		String nodeStr = (String) myStore.get(whoAmI);
		String userScript = (String) myStore.get(PKQLEnum.MATH_FUN);
		userScript = userScript.substring(1, userScript.length()-1).replace("<code>", ""); // Need to remove brackets and code delimiters, create new process in postfix for text so that we can extract text without delimiters
		H2Frame frame = null;
		RRunner r = null;
		
		if (myStore.get(PKQLEnum.G) instanceof H2Frame) {
			frame = (H2Frame) myStore.get(PKQLEnum.G);
		} else {
//			frame = TableDataFrameFactory.convertToH2Frame((TinkerFrame)myStore.get(PKQLEnum.G));
			myStore.put(nodeStr, "Error: Dataframe must be in Grid format.");
			myStore.put("STATUS", STATUS.ERROR);
			return null;
		}
		
		try {
			r = frame.getRRunner();
		} catch (RserveException e) {
			myStore.put(nodeStr, "Error: R server is down.");
			myStore.put("STATUS", STATUS.ERROR);
			e.printStackTrace();
		} catch (SQLException e) {
			myStore.put(nodeStr, "Error: Invalid database connection.");
			myStore.put("STATUS", STATUS.ERROR);
			e.printStackTrace();
		}
		
		if(!r.getDataframeExists()) {
			try {
				r.createDefaultDataframe();
				r.setDataframeExists(true);
			} catch (RserveException e) {
				e.printStackTrace();
				myStore.put(nodeStr, "Error: Could not store data in R.");
				myStore.put("STATUS", STATUS.ERROR);
			}
		}
		
		Object result = r.evaluateScript(userScript);
		myStore.put(nodeStr, result);
		if (r.getScriptRanSuccessfully()) {
			myStore.put("STATUS", STATUS.SUCCESS);
		} else {
			myStore.put("STATUS", STATUS.ERROR);
		}
		return null;
	}

}
