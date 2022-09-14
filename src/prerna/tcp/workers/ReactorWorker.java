package prerna.tcp.workers;

import prerna.om.Insight;
import prerna.project.api.IProject;
import prerna.project.impl.Project;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.IReactor;
import prerna.tcp.PayloadStruct;
import prerna.tcp.SocketServerHandler;

public class ReactorWorker implements Runnable {
	
	// responsible for doing all of the work from an engine's perspective
	// the server sends information to semoss core to execute something
	// this thread will work through in terms of executing it
	// and then send the response back
	SocketServerHandler ssh = null;
	PayloadStruct ps = null;
	Insight insight = null;
	
	public ReactorWorker(SocketServerHandler ssh, PayloadStruct ps, Insight insight)
	{
		this.ssh = ssh;
		this.ps = ps;
		this.insight = insight;
	}

	@Override
	public void run() 
	{
		// get the name from object id
		// instantiate the class
		// set the noun metadata ?
		// call the execute method
		
		try
		{
			// TODO Auto-generated method stub
			String reactorName = ps.objId;
			ps.response = true;
			
			// get the project
			// Project serves no purpose other than just giving me the reactor
			Project project = new Project();
			project.setProjectId(insight.getProjectId());
			project.setProjectName(insight.getProjectName());
			// dont give me a wrapper.. give me the real reactor
			project.setCore(false);
			
			// I dont know if I can do this or I have to use that jar class loader
			IReactor reactor = project.getReactor(reactorName, null);
			reactor.setInsight(insight);
			reactor.setNounStore((NounStore)ps.payload[0]);
			
			// execute
			NounMetadata nmd = reactor.execute();
			
			// return the response
			ps.payload = new Object[] {nmd};
			ps.payloadClasses = new Class[] {NounMetadata.class};
			
		}catch(Exception ex)
		{
			ex.printStackTrace();
			ps.ex = ex.getLocalizedMessage();
			ps.response = true;
		}
		ssh.writeResponse(ps);
	}
}
