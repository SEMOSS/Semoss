package prerna.sablecc2;

import prerna.sablecc2.reactor.IReactor;

public class PlannerTranslation extends Translation {

	protected void deInitReactor()
	{
		if(curReactor != null)
		{
			// merge up and update the plan
			curReactor.mergeUp();
			curReactor.updatePlan();

			// get the parent
			Object parent = curReactor.Out();
						
			// set the parent as the curReactor if it is present
			if(parent != null && parent instanceof IReactor) {
				curReactor = (IReactor)parent;
			} else {
				curReactor = null;
			}
		}
	}

}
