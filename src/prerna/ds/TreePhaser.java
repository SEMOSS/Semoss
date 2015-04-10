package prerna.ds;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Phaser;

public class TreePhaser extends Phaser {

	boolean terminate;
	SimpleTreeBuilder builder = null;
	ExecutorService service = null;
	long startTime, endTime;
	
	public TreePhaser(SimpleTreeBuilder builder, ExecutorService service)
	{
		this.builder = builder;
		this.service = service;
		startTime = builder.printTime();
	}
	
	public void setTerminate(boolean terminate)
	{
		this.terminate = terminate;
	}
	
	@Override
	// triggers to say what needs to happen when they all advance to the next one
	public boolean onAdvance(int phase, int registeredParties)
	{	
		// trigger next
		boolean stop = builder.triggerNext();
		
		// if the idea is to stop then shutdown the service
		if(stop)
		{
			service.shutdown();
			long endTime = builder.printTime();
			long timeToCreateTree = (endTime - startTime)/1000;
			System.out.println("Time Spent... seconds " + timeToCreateTree);
			
			try {
				BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
				System.out.println("Want to flatten ?");
				reader.readLine();
				builder.flattenFromType("TYPE0");
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
		System.err.println("Trigger.. Complete");
		return stop;
		
	}
}
