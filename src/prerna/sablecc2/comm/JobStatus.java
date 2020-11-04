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

	String value = "";

	JobStatus(String value) {
		this.value = value;
	}
	
	public String toString() {
		return this.value;
	}
}
