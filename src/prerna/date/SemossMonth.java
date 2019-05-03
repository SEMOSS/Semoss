package prerna.date;

public class SemossMonth {

	private int numMonths = 1;
	
	public SemossMonth(String numMonths) {
		if(numMonths != null) {
			this.numMonths = Integer.parseInt(numMonths);
		}
	}
	
	public SemossMonth(int numMonths) {
		this.numMonths = numMonths;
	}
	
	public int getNumMonths() {
		return this.numMonths;
	}
}
