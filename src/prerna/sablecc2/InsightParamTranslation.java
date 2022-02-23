package prerna.sablecc2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import prerna.sablecc2.analysis.DepthFirstAdapter;
import prerna.sablecc2.node.AOperation;
import prerna.sablecc2.node.AOtherOpInput;
import prerna.sablecc2.node.ARoutineConfiguration;
import prerna.sablecc2.node.POpInput;
import prerna.sablecc2.node.POtherOpInput;
import prerna.sablecc2.node.PRoutine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;
import prerna.util.gson.GsonUtility;

public class InsightParamTranslation extends DepthFirstAdapter {

	private static final Logger LOGGER = LogManager.getLogger(InsightParamTranslation.class.getName());
	private static Gson GSON = GsonUtility.getDefaultGson();

	private boolean isParam = false;
	private Map<String, Object> viewOptionsMap = new HashMap<String, Object>();

	private boolean notCacheable = false;

	// require a single panel
	private Set<String> panelsCreated = new HashSet<String>();
	
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
        if(reactorId.equals("AddPanel")) {
			// store order of panel creation
			POpInput input = node.getOpInput();
			if(input == null) {
				// person is doing AddPanel() by itself
	        	panelsCreated.add("randomPanel_" + UUID.randomUUID().toString());
			} else {
	        	String panel = input.toString().trim();
	        	panelsCreated.add(panel);
			}
			
        } else if (reactorId.equals("Clone")) {
			POpInput closePanelInput = node.getOpInput();
			String panel = closePanelInput.toString().trim();
        	panelsCreated.add(panel);

		} else if(reactorId.equals("SetPanelView")) {
        	String view = node.getOpInput().toString().trim();
        	if(view.equals("\"param\"")) {
        		notCacheable = true;

        		isParam = true;
        		// need to parse to get the json view
        		// which is sent as a string
        		String viewOptions = null;
    			String encodedViewOptions = null;
        		LinkedList<POtherOpInput> otherInputs = node.getOtherOpInput();
        		if(!otherInputs.isEmpty()) {
        			POtherOpInput otherIn = otherInputs.get(0);
        			if(otherIn instanceof AOtherOpInput) {
        				AOtherOpInput otherOpInput = (AOtherOpInput) otherIn;
        				encodedViewOptions = otherOpInput.getOpInput().toString();
        			}
        			if(encodedViewOptions != null) {
        				encodedViewOptions = PixelUtility.removeSurroundingQuotes(encodedViewOptions);
        				viewOptions = Utility.decodeURIComponent(encodedViewOptions);
        				if(viewOptions != null && !viewOptions.isEmpty()) {
        					try {
        						this.viewOptionsMap = GSON.fromJson(viewOptions, Map.class);
        					} catch(JsonSyntaxException e) {
        						throw new SemossPixelException(new NounMetadata("Panel view is not in a valid JSON format after decoding", 
        								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
        					}
        				}
        			}
        		}
        	} else if(view.equals("\"default-handle\"")) {
        		notCacheable = true;
        	} else if(view.equals("\"grid-delta\"")) {
        		notCacheable = true;
        	}
        }
	}

	public boolean notCacheable() {
		if(panelsCreated.size() == 1 && notCacheable) {
			return true;
		}
		return false;
	}
	
	public Map<String, Object> getPanelViewJson() {
		if(panelsCreated.size() == 1 && isParam) {
			return this.viewOptionsMap;
		}
		
		return null;
	}

}
