package prerna.sablecc2.reactor.storage;

import java.util.Iterator;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.sablecc2.om.Job;
import prerna.sablecc2.om.NounMetadata;
import prerna.sablecc2.om.PkslDataTypes;
import prerna.sablecc2.reactor.AbstractReactor;

public class LoadClient extends AbstractReactor {

	private static final Logger LOGGER = LogManager.getLogger(LoadClient.class.getName());

	//TODO: find a common place to put these
	public static final String STORE_NOUN = "store";
	public static final String ENGINE_NOUN = "engine";
	public static final String CLIENT_NOUN = "client";
	public static final String SCENARIO_NOUN = "scenario";
	public static final String VERSION_NOUN = "version";

	@Override
	public void In() {
        curNoun("all");
	}

	@Override
	public Object Out() {
		return parentReactor;
	}
	
	@Override
	public NounMetadata execute()
	{
		//TODO: need to implement
		
		return null;
	}
	
	private Iterator getIterator() {
		List<Object> jobs = this.curRow.getColumnsOfType(PkslDataTypes.JOB);
		if(jobs != null && jobs.size() > 0) {
			Job job = (Job)jobs.get(0);
			return job.getIterator();
		}

		Job job = (Job)this.getNounStore().getNoun(PkslDataTypes.JOB.toString()).get(0);
		return job.getIterator();
	}
}
