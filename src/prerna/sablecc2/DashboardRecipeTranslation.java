package prerna.sablecc2;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.PRoutine;

public class DashboardRecipeTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(DashboardRecipeTranslation.class.getName());
	private boolean isDashboard = false;
	
	@Override
	public void caseARoutineConfiguration(ARoutineConfiguration node) {
		List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
        for(PRoutine e : copy) {
//        	String expression = e.toString();
//        	LOGGER.info("Processing " + expression);
        	e.apply(this);
        }
	}
	
	
	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);
		String reactorId = node.getId().toString().trim();
		if (reactorId.equals("DashboardInsightConfig")) {
			isDashboard = true;
		}
	}

	public boolean isDashboard() {
		return isDashboard;
	}
}

