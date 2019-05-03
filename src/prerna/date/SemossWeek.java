package prerna.date;

public class SemossWeek {

	private int numWeeks = 1;
	
	public SemossWeek(String numWeeks) {
		if(numWeeks != null) {
			this.numWeeks = Integer.parseInt(numWeeks);
		}
	}
	
	public SemossWeek(int numWeeks) {
		this.numWeeks = numWeeks;
	}
	
	public int getNumWeeks() {
		return this.numWeeks;
	}
	
}
