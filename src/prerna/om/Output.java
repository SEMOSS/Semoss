package prerna.om;

import java.util.Hashtable;

// a class that keeps the output
// typically it keeps the data
// the insight that was used to run it


public class Output {

	// the insight that is being utilized
	Insight insight = null;
	
	// modified SPARQL
	
	
	// the data for the run
	Object data = null;
	
	// parameters utilized for this output
	Hashtable paramNames = new Hashtable();
	
	// add action
	// following are the different types of actions that can be performed
	// extending 
	// overlaying 
	// I will not worry about this right now
	
	
}
