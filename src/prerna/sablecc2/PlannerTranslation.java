package prerna.sablecc2;

import prerna.sablecc2.reactor.IReactor;

public class PlannerTranslation extends Translation {

	protected void deInitReactor()
	{
		if(curReactor != null)
		{
			// get the parent
			Object parent = curReactor.Out();
			// update the plan
			curReactor.updatePlan();

			// set the parent as the curReactor if it is present
			if(parent != null && parent instanceof IReactor) {
				curReactor = (IReactor)parent;
			} else {
				curReactor = null;
			}
		}
	}

}
