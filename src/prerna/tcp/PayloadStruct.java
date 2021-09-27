package prerna.tcp;

import java.io.Serializable;

public class PayloadStruct implements Serializable{
	
	public String epoc = null;
	public enum ENGINE {R, PYTHON, CHROME, ECHO};
	public ENGINE engine = ENGINE.R; // setting default to R
	public String methodName = "method";
	public Object [] payload = null;
	public Class [] payloadClasses = null;
	public String ex = null;
	public boolean processed = false;
	public boolean longRunning = false;
	public String env = null;
	
	// this is a little bit of a complex logic
	// the idea here is say you hve something that is not serializable
	// you can keep it on that end and then work it through that
	public String [] inputAlias = null; // this should be the same size as payload
	public String aliasReturn = null;
	
	public boolean hasReturn = true; // if it is a void set this to true
	
}
