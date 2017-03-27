package prerna.sablecc2.reactor;

public class IfReactor extends AbstractReactor {

	@Override
	public void In() {
		// TODO Auto-generated method stub
        curNoun("all");

	}

	@Override
	public Object Out() {
		// TODO Auto-generated method stub
		System.out.println("Signature " + signature);
		System.out.println("Cur Row is >> " + this.curRow);
		System.out.println("In the out of IF !!");
		return parentReactor;
	}

	@Override
	protected void mergeUp() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void updatePlan() {
		// TODO Auto-generated method stub

	}
	
	// execute it
	// once again this would be abstract
	public Object execute()
	{
		System.out.println("Execute the method.. " + signature);
		System.out.println("Printing NOUN Store so far.. " + store);
		return null;
	}


}
