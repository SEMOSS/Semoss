package prerna.ui.components.specific.ousd;

import prerna.engine.api.IDatabase;

public interface ITimelineGenerator {

	public void createTimeline();
	
	public void createTimeline(IDatabase engine);
	
	public void createTimeline(IDatabase engine, String owner);
	
	public OUSDTimeline getTimeline();
	
}
