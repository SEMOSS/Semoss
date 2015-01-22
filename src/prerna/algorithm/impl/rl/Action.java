package prerna.algorithm.impl.rl;

import java.util.ArrayList;
import java.util.Hashtable;

/**
 * Action is part of the Reinforcement Learning module.
 * It specifies a specific type of movement between states that occur in the problem.
 * A single action can have 1 or more options for movement that occur with a specified probability.
 * Each option leads to a single next state, but multiple options may lead to the same state.
 * @author ksmart
 */
public class Action {

	protected String id;
	private ArrayList<String> optionsList; //list of all the possible options that can be taken
	private Hashtable<String, Double> probabilityHash; //probability of selecting each option. all should add up to 1

	public Action(String id) {
		this.id = id;
		this.optionsList = new ArrayList<String>();
		this.probabilityHash = new Hashtable<String, Double>();
	}
	
	public void addOption(String optionName, double probability) {
		optionsList.add(optionName);
		probabilityHash.put(optionName, probability);
	}

	public Hashtable<String,State> calculateNextStates(ArrayList<State> states, State currState) {
		return null;
	}
	
	public double getProbablity(String option) {
		return probabilityHash.get(option);
	}
	
	public String getID() {
		return id;
	}
	
	@Override
	public boolean equals(Object obj) {
		return this.id.equals(((Action)obj).getID());
	}
}
