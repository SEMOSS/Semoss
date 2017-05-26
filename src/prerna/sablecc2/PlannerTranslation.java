package prerna.sablecc2;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.reactor.IReactor;

public class PlannerTranslation extends Translation {

	private static final Logger LOGGER = LogManager.getLogger(PlannerTranslation.class.getName());

	protected void deInitReactor()
	{
		if(curReactor != null)
		{
			// merge up and update the plan
			try {
				curReactor.mergeUp();
				curReactor.updatePlan();
			} catch(Exception e) {
				LOGGER.error(e.getMessage());
			}
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
	
	protected void postProcess() {
		// do nothing
	}

}
