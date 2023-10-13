package prerna.reactor;

import prerna.om.Insight;

public interface IParallelWorker {

	// main interface to implement threadable activity
	public void setInisight(Insight insight);
	
	// the main method that will be run as a thread
	public void run();
}
