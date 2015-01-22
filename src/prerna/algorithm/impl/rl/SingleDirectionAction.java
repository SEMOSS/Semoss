package prerna.algorithm.impl.rl;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author ksmart
 */
public class SingleDirectionAction extends MultiDirectionAction{
	static final Logger LOGGER = LogManager.getLogger(SingleDirectionAction.class.getName());
	
	public SingleDirectionAction(String id, int xSize, int ySize) {
		super(id,xSize,ySize);
		
		if(!id.equals(LEFT_KEY) && !id.equals(RIGHT_KEY) && !id.equals(UP_KEY) && !id.equals(DOWN_KEY)) {
			LOGGER.error("SingleDirectionAction cannot have the id specified: "+id);
			LOGGER.error("ID names for this are limited to: "+LEFT_KEY+", "+RIGHT_KEY+", "+UP_KEY+", "+DOWN_KEY);
			return;
		}
	}

	@Override
	public Hashtable<String,State> calculateNextStates(ArrayList<State> states, State currState) {
		Hashtable<String,State> nextStateHash = new Hashtable<String,State>();
		
		State leftState = calculateNextState(states,currState,id);
		nextStateHash.put(id,leftState);
		
		return nextStateHash;
	}

}
