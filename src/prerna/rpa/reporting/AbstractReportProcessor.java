package prerna.rpa.reporting;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.rpa.RPAUtil;
import prerna.util.Utility;

/**
 * Framework for processing a multiple reports asynchronously.
 * {@link AbstractReportProcess}es are submitted to a {@link CompletionService}.
 * 
 * @author tbanach
 *
 */
public abstract class AbstractReportProcessor implements Runnable {
	
	private static final Logger LOGGER = LogManager.getLogger(AbstractReportProcessor.class.getName());

	private static final long TERMINATION_TIMEOUT = 60L;

	private final String reportDirectory;
	private final ExecutorService executorService;
	private final long shutdownTimeout; // seconds
	private final Set<String> reports = new HashSet<>();
	private final int nTotal;
	
	private int nFinished = 0;
	private int nAlreadyProcessed = 0;
	private int nProcessed = 0;
	private int nFailed = 0;
	
	private JedisDataExceptionHandler handler = new JedisDataExceptionHandler();
	
	private volatile boolean terminated = false;
	
	/**
	 * 
	 * @param reportDirectory
	 *            - the full path to the directory containing reports
	 * @param reportTester
	 *            - developers can compose a lambda expression to test whether or
	 *            not to process a report in the report directory
	 * @param nThreads
	 *            - number of threads in the {@link CompletionService}
	 * @param shutdownTimeout
	 *            - if interrupted, how long to wait for already-submitted processes
	 *            to complete execution before a forceful shutdown
	 * @throws ReportProcessingException
	 *             if the reportDirectory provided is not a directory
	 */
	public AbstractReportProcessor(String reportDirectory, Predicate<String> reportTester, int nThreads, long shutdownTimeout) throws ReportProcessingException {
		this.reportDirectory = reportDirectory;
		this.executorService = Executors.newFixedThreadPool(nThreads);
		this.shutdownTimeout = shutdownTimeout;
		
		// Populate the stack
		File directory = new File(reportDirectory);
		
		// Throw an exception if not a directory
		if (!directory.isDirectory()) {
			throw new ReportProcessingException(reportDirectory + " is not a directory.");
		}
		
		// Loop through and add all eligible files for processing
		for (File file : directory.listFiles()) {
			if (reportTester.test(file.getName())) {
				reports.add(file.getAbsolutePath());
			}
		}
		nTotal = reports.size();
	}
	
	/**
	 * Kickoff the {@link CompletionService} of {@link AbstractReportProcess}es.
	 * @return boolean - true only if all processes successfully completed.
	 */
	public boolean processAll() {
		if (terminated) return false;
		LOGGER.info("Processing reports in " + reportDirectory + ".");
		CompletionService<Integer> completionService = new ExecutorCompletionService<>(executorService);
		
		// Start monitoring for Jedis data exceptions
		Thread handlerThread = new Thread(handler);
		handlerThread.start();
		
		// Submit each report for processing
		long startTime = System.currentTimeMillis();
		for (String reportPath : reports) {
			try {
				AbstractReportProcess process = giveProcess(reportPath);
				process.setJedisDataExceptionHandler(handler);
				completionService.submit(process);
			} catch (ReportProcessingException e) {
				LOGGER.error("Failed to submit " + Utility.cleanLogString(reportPath) + " for processing.", e);
			} catch (RejectedExecutionException e) {
				
				// If shutdownAndAwaitTermination method is called while submitting reports,
				// this exception will occur
				LOGGER.warn("Unable to submit " + Utility.cleanLogString(reportPath) + " for processing; this processor is no longer accepting new reports.");
			}
		}
		
		// Collect all the results
		boolean completed = false;
		while(!completed) {
			try {
				int status = completionService.take().get();
				if (status == 0) nAlreadyProcessed++;
				if (status == 1) nProcessed++;
				if (status == -1) nFailed++;
			} catch (InterruptedException e) {
				shutdownAndAwaitTermination();
				
				// Preserve interrupt status
				Thread.currentThread().interrupt();
				break;
			} catch (ExecutionException e) {
				LOGGER.error("An unexpected error occurred while processing one of the reports. ", e);
				nFailed++;
			}
			nFinished++;
			completed = nFinished == nTotal;
			LOGGER.info(nFinished + "/" + nTotal + " reports finished processing.");
		}
		LOGGER.info("Finished processing reports in " + reportDirectory);
		LOGGER.info("Elapsed time " + RPAUtil.minutesSinceStartTime(startTime) + " minutes.");
		LOGGER.info(nFinished + "/" + nTotal + " reports finished processing.");
		LOGGER.info(nAlreadyProcessed + "/" + nTotal + " reports already processed.");
		LOGGER.info(nProcessed + "/" + nTotal + " reports newly processed.");
		LOGGER.info(nFailed + "/" + nTotal + " reports failed to process.");

		// Shutdown the service
		executorService.shutdown();
		
		// Release the handler
		handler.release();
		
		// Return true only if there were no failures
		return (nFailed == 0);
	}
	
	/**
	 * Gracefully shuts down processing threads. Any reports that are yet to be
	 * processed after shutting down can be safely worked later.
	 */
	public void shutdownAndAwaitTermination() {
		shutdownAndAwaitTermination(TimeUnit.SECONDS);
	}
	
	// https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
	/**
	 * Gracefully shuts down processing threads. Any reports that are yet to be
	 * processed after shutting down can be safely worked later.
	 * 
	 * @param shutdownTimeUnit
	 *            - What unit of time the shutdownTimeout defined in the constructor
	 *            represents, needed for JUnits
	 */
	public void shutdownAndAwaitTermination(TimeUnit shutdownTimeUnit) {
		terminated = true;
		LOGGER.info("Received request to shutdown the processor for reports in " + reportDirectory + ".");
		LOGGER.info("Disabling new tasks from being submitted.");
		executorService.shutdown(); // Disable new tasks from being submitted
		try {
			
			// Wait a while for existing tasks to terminate
			LOGGER.info("Waiting for existing tasks to terminate.");
			if (!executorService.awaitTermination(shutdownTimeout, shutdownTimeUnit)) {
				LOGGER.warn("Not all actively-processing reports finished within the shutdown period of " + shutdownTimeout + " " + shutdownTimeUnit.toString().toLowerCase() + ".");
				LOGGER.warn("Will cancel actively-processing reports.");
				executorService.shutdownNow(); // Cancel currently executing tasks
				
				// Wait a while for tasks to respond to being cancelled
				if (!executorService.awaitTermination(TERMINATION_TIMEOUT, TimeUnit.SECONDS)) {
					LOGGER.error("Failed to terminate actively-processing reports within " + TERMINATION_TIMEOUT + " seconds.");
				}
			} else {
				LOGGER.info("All actively-processing reports finished processing within the shutdown period of " + shutdownTimeout + " " + shutdownTimeUnit.toString().toLowerCase() + ".");
			}
		} catch (InterruptedException e) {
			
			// (Re-)Cancel if current thread also interrupted
			executorService.shutdownNow();
			
			// Preserve interrupt status
			Thread.currentThread().interrupt();
		}
		LOGGER.info("Finished shutting down processor for reports in " + reportDirectory + ".");
		LOGGER.info("Status as of shutdown: ");
		LOGGER.info(nFinished + "/" + nTotal + " reports finished processing.");
		LOGGER.info(nAlreadyProcessed + "/" + nTotal + " reports already processed.");
		LOGGER.info(nProcessed + "/" + nTotal + " reports newly processed.");
		LOGGER.info(nFailed + "/" + nTotal + " reports failed to process.");
		
		// Release the handler
		handler.release();
	}

	/**
	 * Forces subclasses to give the appropriate implementation of an
	 * {@link AbstractReportProcess}.
	 * 
	 * @param reportPath
	 * @return a concrete implementation of an {@link AbstractReportProcess}
	 * @throws ReportProcessingException
	 *             if the process fails to initialize
	 */
	protected abstract AbstractReportProcess giveProcess(String reportPath) throws ReportProcessingException;
	
	// For convenience, make this runnable
	@Override
	public void run() {
		processAll();
	}
	
}
