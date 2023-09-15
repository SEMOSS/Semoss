package prerna.ui.components.specific.ousd;

import prerna.annotations.BREAKOUT;
import prerna.engine.api.IDatabaseEngine;

@BREAKOUT
public interface ITimelineGenerator {

	public void createTimeline();
	
	public void createTimeline(IDatabaseEngine engine);
	
	public void createTimeline(IDatabaseEngine engine, String owner);
	
	public OUSDTimeline getTimeline();
	
}
