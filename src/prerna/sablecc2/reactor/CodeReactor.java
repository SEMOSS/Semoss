package prerna.sablecc2.reactor;

public class CodeReactor extends SampleReactor {
	
	public Object Out()
	{
		updatePlan();
		System.out.println("Out of as reactor");
		return parentReactor;
	}

	public void updatePlan()
	{
		// set the code in parent
		// done
		if(parentReactor != null)
			parentReactor.setProp("CODE", this.signature);
	}
}