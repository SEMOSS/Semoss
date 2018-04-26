package prerna.om;

public class SentimentAnalysis {

	private String sentence;
	private double magnitude;
	private double score;
	
	public SentimentAnalysis() {
		
	}
	
	/*
	 * This is just a struct
	 * Define setters and getters for the class variables
	 */
	
	public String getSentence() {
		return sentence;
	}
	public void setSentence(String sentence) {
		this.sentence = sentence;
	}
	public double getMagnitude() {
		return magnitude;
	}
	public void setMagnitude(double magnitude) {
		this.magnitude = magnitude;
	}
	public double getScore() {
		return score;
	}
	public void setScore(double score) {
		this.score = score;
	}
	
	
	
}
