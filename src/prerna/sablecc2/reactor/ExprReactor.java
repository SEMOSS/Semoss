package prerna.sablecc2.reactor;


public class ExprReactor extends SampleReactor {

	public void In()
	{
		if(this.parentReactor != null && this.parentReactor.getName().equalsIgnoreCase("OPERATION_FORMULA"))
		{
			this.store = this.parentReactor.getNounStore();
			curRow = store.getNoun("all");
		}
		else
			curNoun("all");

	}
	
	public Object Out()
	{
		System.out.println("Calling the out of" + operationName);
		System.out.println("Calling the out of " + reactorName);
		// if the operation is fully sql-able etc
		// and it is a map operation.. in this place it should merge with the parent
		// there are 2 cases here
		// a. I can assimilate with the parent and let it continue to rip
		// b. I have to finish processing this before I give it off to parent
		
		// additionally.. if I do see a parent reactor.. I should add this as the input to the parent
		// so that we know what order to execute it
		
		if(this.parentReactor != null && !this.parentReactor.getName().equalsIgnoreCase("OPERATION_FORMULA"))
		{
			// align to plan
			// not sure I need this right now to add something to itself ?
			// updatePlan();
		}
		if(this.type != IReactor.TYPE.REDUCE && this.store.isSQL() && this.parentReactor != null && !this.parentReactor.getName().equalsIgnoreCase("OPERATION_FORMULA"))
		{
			// 2 more scenarios here
			// if parent reactor is not null
			// merge
			// if not execute it
			// if the whole thing is done through SQL, then just add the expression
			if(this.parentReactor != null)
			{
				mergeUp();
				return parentReactor;
			}
			// else assimilated with the other execute
/*			else
			{
				// execute it
			}
*/		
		}
		else if(parentReactor == null)
		{
			// execute it
			//return execute();
		}
		else if(parentReactor != null) return parentReactor;
		// else all the merging has already happened
		return null;
	}
}
