package prerna.sablecc2;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AConfiguration;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.POpInput;
import prerna.sablecc2.node.PRoutine;

public class InsightParamTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(InsightParamTranslation.class.getName());
	private boolean hasParam = false;
	
	private Set<String> panelsCreated = new HashSet<String>();
	
	
	@Override
	public void caseAConfiguration(AConfiguration node) {
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
        if(reactorId.equals("AddPanel")) {
			// store order of panel creation
			POpInput input = node.getOpInput();
        	String panel = input.toString().trim();
        	panelsCreated.add(panel);

        }  else if (reactorId.equals("Clone")) {
			POpInput closePanelInput = node.getOpInput();
			String panel = closePanelInput.toString().trim();
        	panelsCreated.add(panel);

		} else if(reactorId.equals("SetPanelView")) {
        	String view = node.getOpInput().toString().trim();
        	if(view.equals("\"param\"")) {
        		hasParam = true;
        	} else if(view.equals("\"default-handle\"")) {
        		hasParam = true;
        	} else if(view.equals("\"grid-delta\"")) {
        		hasParam = true;
        	}
        }
	}

	public boolean hasParam() {
		if(panelsCreated.size() == 1 && hasParam) {
			return true;
		}
		return false;
	}

}
