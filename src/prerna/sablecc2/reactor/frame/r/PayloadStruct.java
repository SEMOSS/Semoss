package prerna.sablecc2.reactor.frame.r;

import java.io.Serializable;

public class PayloadStruct implements Serializable{
	
	public String epoc = null;
	public enum ENGINE {R, PYTHON};
	public ENGINE engine = ENGINE.R; // setting default to R
	public String methodName = "method";
	public Object [] payload = null;
	public Class [] payloadClasses = null;
	public String ex = null;
	public boolean processed = false;
	public boolean longRunning = false;
	public String env = null;
	
	public boolean hasReturn = true; // if it is a void set this to true
	
}
