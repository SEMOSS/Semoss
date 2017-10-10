package prerna.sablecc2.reactor.frame.r.util;

import org.rosuda.JRI.REXP;

public class RJavaRserveTranslator extends AbstractRJavaTranslator {

	/**
	 * Have this be protected since we want the control to be based on the 
	 * RJavaTranslator to determine which class to generate
	 */
	protected RJavaRserveTranslator() {
		
	}
	
	@Override
	public REXP executeR(String rScript) {

		return null; 
	}

	@Override
	public int[] getIntArray(String rScript) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startR() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String[] getStringArray(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(String script) {
		// TODO Auto-generated method stub
		return null;
	}

	
}
