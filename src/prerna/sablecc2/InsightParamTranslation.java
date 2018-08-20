package prerna.sablecc2;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.PRoutine;

public class InsightParamTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(InsightParamTranslation.class.getName());
	private boolean hasParam = false;
	
	@Override
	public void caseAConfiguration(AConfiguration node) {
        List<PRoutine> copy = new ArrayList<PRoutine>(node.getRoutine());
        for(PRoutine e : copy) {
        	String expression = e.toString();
        	LOGGER.info("Processing " + expression);
        	e.apply(this);
        }
	}
	
	@Override
	public void inAOperation(AOperation node) {
		defaultIn(node);
        
        String reactorId = node.getId().toString().trim();
        if(reactorId.equals("SetPanelView")) {
        	String view = node.getOpInput().toString().trim();
        	if(view.equals("\"param\"")) {
        		hasParam = true;
        	} else if(view.equals("\"default-handle\"")) {
        		hasParam = true;
        	}
        }
	}

	public boolean hasParam() {
		return hasParam;
	}

}
