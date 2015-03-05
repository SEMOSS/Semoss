package prerna.poi.main.insights;

import java.util.Hashtable;

public class InsightRule {

	private Hashtable<String, Hashtable<String, Object>> constraints;
	
	private boolean hasAggregation = false;
	private String question = "";
	private String output = "";
	
	public InsightRule() {
		constraints = new Hashtable<String, Hashtable<String, Object>>();
	}
	
	
	/**
	 * Adds to the constants table containing the constraints for the insight rule
	 * @param param					The param name the rule applies to
	 * @param constraintType		The type of the constraint
	 * @param value					The value of the constraint
	 */
	public void addConstraint(String param, String constraintType, String value) {
		Hashtable<String, Object> constraintValueHash;
		if(constraints.containsKey(param)) {
			constraintValueHash = constraints.get(param);
			constraintValueHash.put(constraintType, value);
		} else {
			constraintValueHash = new Hashtable<String, Object>();
			constraintValueHash.put(constraintType, value);
			constraints.put(param, constraintValueHash);
		}
	}

	public Hashtable<String, Hashtable<String, Object>> getConstraints() {
		return constraints;
	}

	public void setConstraints(
			Hashtable<String, Hashtable<String, Object>> constraints) {
		this.constraints = constraints;
	}

	public boolean isHasAggregation() {
		return hasAggregation;
	}

	public void setHasAggregation(boolean hasAggregation) {
		this.hasAggregation = hasAggregation;
	}

	public String getQuestion() {
		return question;
	}

	public void setQuestion(String question) {
		this.question = question;
	}

	public String getOutput() {
		return output;
	}

	public void setOutput(String output) {
		this.output = output;
	}
}
