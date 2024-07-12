package prerna.tcp.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bouncycastle.util.Arrays;

import prerna.om.Insight;
import prerna.sablecc2.comm.JobManager;
import prerna.util.Constants;

public class ErrorSenderThread extends Thread
{
	
	private static final Logger classLogger = LogManager.getLogger(ErrorSenderThread.class);

	FileInputStream pis = null;
	String file = null;
	boolean errored = false;
	Object monitor = new Object();
	boolean processComplete = false;
	Insight in = null;
	String core = "server";
	
				
	public void setFile(String file)
	{
		this.file = file;
		this.file = file.replace("\"", "");
		this.file = this.file.trim();

		processComplete = false;
		//WatchService watchService = FileSystems.getDefault().newWatchService();
		System.err.println("Starting session........ ");
	}
	
	public void stopSession()
	{
		this.processComplete = true;
		this.file = null;
		JobManager.getManager().addStdOut(in.getInsightId(), "Execution complete...");		
		//JobManager.getManager().flushJob(in.getInsightId());
	}
	
	public void setInsight(Insight in)
	{
		this.in = in;
	}
	
	
	@Override
	public void run()
	{
		// waits until the set file is called
		// when called will start this thread
		
		// sleeps for 2 seconds first
		// after that starts reading the file and starts pumping the output until 
		JobManager.getManager().addStdOut(in.getInsightId(), "Connecting to " + core);
		synchronized(monitor)
		{
			try {
				//monitor.wait();
				
				// wait for error file
				// if one was never created but process completed
				// kill this thread
				System.err.println("Waiting for file.. " + file);
				File errorFile = new File(file);
				JobManager.getManager().addStdOut(in.getInsightId(), "Starting Execution.. ");
				while(!errorFile.exists() && !processComplete)
				{
					// wait for python process to create it
					// I can trigger from the server
					// sleeping for 200
					try
					{
						Thread.sleep(200);
					}catch(Exception ex)
					{
						// ignored
					}
				}
				JobManager.getManager().addStdOut(in.getInsightId(), "Execution Started..  ");				
				if(processComplete) // that was quick ok.. 
				{
					JobManager.getManager().addStdOut(in.getInsightId(), "Execution complete  ");				
					return;
				}
				JobManager.getManager().addStdOut(in.getInsightId(), "Printing Output.. ");
				// once the file is there all set
				pis = new FileInputStream(file);
				//pis = new FileInputStream(inputFile);
				
				// read until everything is done
				// and send it as payload to the front end
				while(!processComplete)
				{
				// we need to pump messages right now
					byte [] readBytes = new byte[128];
					int byteRead = pis.read(readBytes); // blocks here
					if(byteRead > 0)
					{
						byte [] realReadBytes = Arrays.copyOf(readBytes, byteRead);
						//pis.reset();
						String data = new String(realReadBytes);
						JobManager.getManager().addStdOut(in.getInsightId(), data);
					}
				}
				JobManager.getManager().addStdOut(in.getInsightId(), "Execution complete  ");				
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				classLogger.error(Constants.STACKTRACE, e);
				processComplete = true;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				classLogger.error(Constants.STACKTRACE, e);
				processComplete = true;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				classLogger.error(Constants.STACKTRACE, e);
				processComplete = true;
			}
		}
	}
	
	public void sendMessage(String message)
	{
		// we could say this can be done directly on job manager.. but
	}
	
}
