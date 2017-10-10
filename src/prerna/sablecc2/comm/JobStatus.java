package prerna.sablecc2.comm;

public enum JobStatus {
	
	
	CREATED ("Created"), SUBMITTED ("Submitted"), INPROGRESS ("In Progress"), PAUSED ("Paused"), COMPLETE ("Complete"), ERROR ("Error");

	String value = "";

	JobStatus(String value)
	{
		this.value = value;
	}
	
	public String toString()
	{
		return this.value;
	}
}
