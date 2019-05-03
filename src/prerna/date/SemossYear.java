package prerna.date;

public class SemossYear {

	private int numYears = 1;
	
	public SemossYear(String numYears) {
		if(numYears != null) {
			this.numYears = Integer.parseInt(numYears);
		}
	}
	
	public SemossYear(int numYears) {
		this.numYears = numYears;
	}
	
	public int getNumYears() {
		return this.numYears;
	}
}
