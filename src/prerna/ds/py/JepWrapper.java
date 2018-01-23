package prerna.ds.py;

import jep.Jep;
import jep.JepException;

/**
 * So, since Jep has this whole thread security thing
 * We are creating Jep within a thread so we can have different threads run the methods
 * Without getting an illegal thread access exception
 * 
 * We have no reason to actually override the run method
 */
public class JepWrapper extends Thread {

	private Jep jep;
	
	/**
	 * Create the Jep for this specific thread
	 */
	public JepWrapper() {
		try {
			this.jep = new Jep(false);
		} catch (JepException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
	}
	
	public void runScript(String fileLocation) {
		try {
			jep.runScript(fileLocation);
		} catch (JepException e) {
			e.printStackTrace();
		}
	}
	
	public Object eval(String singleLineOp) {
		try {
			return jep.eval(singleLineOp);
		} catch (JepException e) {
			e.printStackTrace();
		}
		return null;
	}
	
}
