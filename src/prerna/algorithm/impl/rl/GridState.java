package prerna.algorithm.impl.rl;


/**
 * State is used //TODO class for Reinforced Learning.
 * It stores all of the next states that can be reached from this state
 * as well as the probability of them occurring, and their expected return.
 * @author ksmart
 */
public class GridState extends State{

	private Integer x;
	private Integer y;
	
	public GridState(String id, Double reward,Integer x,Integer y, Boolean terminal) {
		super(id,reward,terminal);
		this.x = x;
		this.y = y;
	}
	
	public Integer getX() {
		return x;
	}
	
	public Integer getY() {
		return y;
	}
}
