package prerna.sablecc2.comm;

public enum JobStatus {

	CREATED ("Created"), 
	SUBMITTED ("Submitted"), 
	INPROGRESS ("InProgress"), 
	STREAMING ("Streaming"), 
	COMPLETE ("Complete"), 
	PAUSED ("Paused"), 
	ERROR ("Error"),
	UNKNOWN_JOB ("UnknownJob");

	private String value = "";

	JobStatus(String value) {
		this.value = value;
	}
	
	public String getValue() {
		return this.value;
	}
	
	public String toString() {
		return this.value;
	}
}
