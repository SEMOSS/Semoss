package prerna.date;

public class SemossDay {

	private int numDays = 1;
	
	public SemossDay(String numDays) {
		if(numDays != null) {
			this.numDays = Integer.parseInt(numDays);
		}
	}
	
	public SemossDay(int numDays) {
		this.numDays = numDays;
	}
	
	public int getNumDays() {
		return this.numDays;
	}
	
}
