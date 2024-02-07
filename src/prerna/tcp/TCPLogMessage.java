package prerna.tcp;

import java.io.Serializable;

public class TCPLogMessage implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3438098999282545733L;

	public String stack;
	public String levelName;
	
	public String message;
	public String name;
	public String lineNumber;
}
