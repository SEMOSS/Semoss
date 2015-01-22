package prerna.algorithm.impl.rl;

import java.util.ArrayList;
import java.util.Hashtable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author ksmart
 */
public class MultiDirectionAction extends Action{
	static final Logger LOGGER = LogManager.getLogger(MultiDirectionAction.class.getName());
	
	private int xSize;
	private int ySize;
	protected static final String LEFT_KEY = "left";
	protected static final String RIGHT_KEY = "right";
	protected static final String UP_KEY = "up";
	protected static final String DOWN_KEY = "down";
	
	public MultiDirectionAction(String id, int xSize, int ySize) {
		super(id);
		this.xSize = xSize;
		this.ySize = ySize;
	}

	@Override
	public Hashtable<String,State> calculateNextStates(ArrayList<State> states, State currState) {
		Hashtable<String,State> nextStateHash = new Hashtable<String,State>();
		
		State leftState = calculateNextState(states,currState,LEFT_KEY);
		nextStateHash.put(LEFT_KEY,leftState);

		State rightState = calculateNextState(states,currState,RIGHT_KEY);
		nextStateHash.put(RIGHT_KEY,rightState);

		State upState = calculateNextState(states,currState,UP_KEY);
		nextStateHash.put(UP_KEY,upState);

		State downState = calculateNextState(states,currState,DOWN_KEY);
		nextStateHash.put(DOWN_KEY,downState);

		return nextStateHash;
	}
	
	protected State calculateNextState(ArrayList<State> states, State currState,String option) {
		int currX = ((GridState)currState).getX();
		int currY = ((GridState)currState).getY();
		int x = ((GridState)currState).getX();
		int y = ((GridState)currState).getY();
		
		//moving left
		if(option.equals(LEFT_KEY)) {
			if(currX > 0)
				x = currX-1;
		}
		
		//moving right
		if(option.equals(RIGHT_KEY)) {
			if(currX < xSize-1)
				x = currX+1;
		}

		//moving up
		if(option.equals(UP_KEY)) {
			if(currY > 0)
				y = currY-1;
		}
		
		//moving down
		if(option.equals(DOWN_KEY)) {
			if(currY < ySize -1)
				y = currY+1;
		}
		
		if(x==xSize-1 && y == ySize -1) {
			x=0;
			y=0;
		}

		State nextState = findState(states,x,y);
		return nextState;
	}
	
	private State findState(ArrayList<State> states,Integer x, Integer y) {
		for(State state : states)
			if(((GridState)state).getX().equals(x) && ((GridState)state).getY().equals(y))
				return state;
		return null;		
	}

}
