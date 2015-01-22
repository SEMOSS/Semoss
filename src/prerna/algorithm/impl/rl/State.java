package prerna.algorithm.impl.rl;

/**
 * State is part of the Reinforcement Learning module.
 * It specifies a single state that can occur in the problem.
 * It stores the id of the state, the reward received for reaching this state, and whether it is terminal.
 * @author ksmart
 */
public class State {

	private String id;
	private double reward; //reward for reaching this state.
	private Boolean terminal;
	
	/**
	 * Creates a new state with an id and a reward
	 * @param id String representing the states id. Should be unique.
	 * @param reward Double representing the value of reaching this state
	 */
	public State(String id, double reward, Boolean terminal) {
		this.id = id;
		this.reward = reward;
		this.terminal = terminal;
	}
	
	public String getID() {
		return id;
	}
	
	public double getReward() {
		return reward;
	}
	
	public Boolean isTerminal() {
		return terminal;
	}
	
	@Override
	public boolean equals(Object obj) {
		return this.id.equals(((State)obj).getID());
	}
}
