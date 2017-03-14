package prerna.sablecc2.reactor;

import java.util.HashMap;
import java.util.Map;

import prerna.sablecc2.reactor.export.FormatReactor;
import prerna.sablecc2.reactor.qs.AverageReactor;
import prerna.sablecc2.reactor.qs.DatabaseReactor;
import prerna.sablecc2.reactor.qs.FrameReactor;
import prerna.sablecc2.reactor.qs.GroupByReactor;
import prerna.sablecc2.reactor.qs.JoinReactor;
import prerna.sablecc2.reactor.qs.LimitReactor;
import prerna.sablecc2.reactor.qs.OffsetReactor;
import prerna.sablecc2.reactor.qs.QueryFilterReactor;
import prerna.sablecc2.reactor.qs.SelectReactor;
import prerna.sablecc2.reactor.qs.SumReactor;

public class ReactorFactory {

	Map<String, String> reactorHash;
	
	//pass in reactors from the frame
	//also need to add in some default reactors
	public ReactorFactory(Map<String, String> reactorHash) {
		this.reactorHash = new HashMap<String, String>();
		this.reactorHash.putAll(reactorHash);
	}
	
	public void addReactors(Map<String, String> reactorHash) {
		if(reactorHash == null) {
			reactorHash = new HashMap<String, String>();
		}
		this.reactorHash.putAll(reactorHash);
	}
	
	public IReactor getReactor(String reactorId) {
		String reactorClassName = getReactorClassName(reactorId);
		try {
			return (IReactor) Class.forName(reactorClassName).newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	private String getReactorClassName(String reactorId) {
		return reactorHash.get(reactorId);
	}
	
	
    public static IReactor getReactor(String reactorId, String nodeString) {
    	IReactor reactor;
    	if(reactorId.trim().equals("Query")) {
    		reactor = new QueryReactor();
    	} else if(reactorId.trim().equals("Import")) {
    		reactor = new ImportDataReactor();
    	} else if(reactorId.trim().equals("Merge")) {
    		reactor = new MergeDataReactor();
    	} else if(reactorId.trim().equals("Group")) {
    		reactor = new GroupByReactor();
    	} else if(reactorId.trim().equals("Database")) {
    		reactor = new DatabaseReactor();
    	} else if(reactorId.trim().equals("Select")) {
    		reactor = new SelectReactor();
    	} else if(reactorId.trim().equals("Limit")) {
    		reactor = new LimitReactor();
    	} else if(reactorId.trim().equals("Offset")) {
    		reactor = new OffsetReactor();
    	} else if(reactorId.trim().equals("Iterate")) {
    		reactor = new IterateReactor();
    	} else if(reactorId.trim().equals("GetData")) {
    		reactor = new GetDataReactor();
    	} else if(reactorId.trim().equals("Frame")) {
    		reactor = new FrameReactor();
    	} else if(reactorId.trim().equals("Join")) {
    		reactor = new JoinReactor();
    	} else if(reactorId.trim().equals("Filter")) {
    		reactor = new QueryFilterReactor();
    	} else if(reactorId.trim().equals("Sum")) {
    		reactor = new SumReactor();
    	} else if(reactorId.trim().equals("Average")) {
    		reactor = new AverageReactor();
    	} else if(reactorId.trim().equals("Collect")) {
    		reactor = new CollectReactor();
    	} else if(reactorId.trim().equals("Job")) {
    		reactor = new JobReactor();
    	} else if(reactorId.trim().equals("Format")) {
    		reactor = new FormatReactor();
    	} else {
    		reactor = new SampleReactor();
    	}
    	reactor.setPKSL(reactorId, nodeString);
    	return reactor;
    	
    }
}
