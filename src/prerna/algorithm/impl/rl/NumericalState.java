package prerna.algorithm.impl.rl;


/**
 * State is used //TODO class for Reinforced Learning.
 * It stores all of the next states that can be reached from this state
 * as well as the probability of them occurring, and their expected return.
 * @author ksmart
 */
public class NumericalState extends State{

	private Integer x;
	
	public NumericalState(String id, double reward,int x, Boolean terminal) {
		super(id,reward,terminal);
		this.x = x;
	}
	
	public Integer getX() {
		return x;
	}
}
