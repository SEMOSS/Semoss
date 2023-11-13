package prerna.util.sql;

import java.util.ArrayList;
import java.util.List;

import prerna.engine.impl.owl.WriteOWLEngine;

public class DatabaseUpdateMetadata {

	private String combinedErrors = null;
	private List<String> successfulUpdates = new ArrayList<>();
	private List<String> failedUpdates = new ArrayList<>();
	// only keep this so we can commit from it at the end
	private transient WriteOWLEngine owlEngine = null;
	
	public void addSuccessfulUpdate(String table) {
		this.successfulUpdates.add(table);
	}
	
	public void addFailedUpdates(String table) {
		this.failedUpdates.add(table);
	}
	
	public WriteOWLEngine getOwlEngine() {
		return owlEngine;
	}
	
	public void setOwlEngine(WriteOWLEngine owlEngine) {
		this.owlEngine = owlEngine;
	}
	
	public String getCombinedErrors() {
		return combinedErrors;
	}
	
	public void setCombinedErrors(String combinedErrors) {
		this.combinedErrors = combinedErrors;
	}
	
	public List<String> getSuccessfulUpdates() {
		return successfulUpdates;
	}
	
	public void setSuccessfulUpdates(List<String> successfulUpdates) {
		this.successfulUpdates = successfulUpdates;
	}
	
	public List<String> getFailedUpdates() {
		return failedUpdates;
	}
	
	public void setFailedUpdates(List<String> failedUpdates) {
		this.failedUpdates = failedUpdates;
	}
}
