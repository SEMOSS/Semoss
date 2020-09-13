package prerna.rpa.quartz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobBatchTimeoutAlarm implements Runnable {
	
	private static final Logger LOGGER = LogManager.getLogger(JobBatchTimeoutAlarm.class.getName());
	
	private final long timeout; // in seconds
	private final JobBatch jobBatch;
	
	private volatile boolean interrupted = false;
	
	// Don't make these static in case there is more than one JobBatch
	private final Object jobMonitor;
	private final Object timeoutMonitor = new Object();
	
	public JobBatchTimeoutAlarm(Object jobMonitor, long timeout, JobBatch jobBatch) {
		this.jobMonitor = jobMonitor;
		this.timeout = timeout;
		this.jobBatch = jobBatch;
	}
	
	@Override
	public void run() {
		synchronized (timeoutMonitor) {
			try {
				
				// The timeout monitor should be released by either:
				//		a) an interruption (jobs completed successfully or job itself was interrupted)
				//		c) a true timeout
				// Only consider as truly timed out if not interrupted in the process
				timeoutMonitor.wait(1000 * timeout);
				if (!interrupted) {
					jobBatch.setTimedOut(true);
					synchronized (jobMonitor) {
						jobMonitor.notifyAll();
					}
				}
			} catch (InterruptedException e) {
				LOGGER.error("Thread for the job batch timeout alarm interrupted in an unexpected manner.", e);
				
				// Preserve interrupt status
				Thread.currentThread().interrupt();
			}
		}
	}
	
	public void interrupt() {
		
		// Notify the waiting timeoutMonitor with the interrupted=true flag
		interrupted = true;
		synchronized (timeoutMonitor) {
			timeoutMonitor.notifyAll();
		}
	}

}
