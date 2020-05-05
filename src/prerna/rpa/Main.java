package prerna.rpa;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobExecutionContext;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.rpa.config.JobConfigParser;
import prerna.rpa.db.jedis.JedisStore;
import prerna.rpa.quartz.SchedulerUtil;
import prerna.rpa.security.Cryptographer;

public class Main {
	
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	
	private static final String NEW_LINE = System.lineSeparator();
	
	private static final String RPA_INTRO = 
			NEW_LINE +
			" __ __ __ __ __ _" + NEW_LINE + 
			"|                | SEMOSS RPA 3.0.0" + NEW_LINE + 
			"|  ( )           |" + NEW_LINE + 
			"|   |  .         | R obotic" + NEW_LINE + 
			"|   |     .      | P rocess" + NEW_LINE + 
			"|   |       ( )  | A utomation" + NEW_LINE + 
			"|   |     .      |" + NEW_LINE + 
			"|   |  .         |" + NEW_LINE + 
			"|  ( )           |" + NEW_LINE + 
			"|__ __ __ __ __ _|_ __ __ __ __ __" + NEW_LINE + 
			"                 |   _   _  _     |" + NEW_LINE + 
			" semoss.org      | | _|.| || |    |" + NEW_LINE + 
			"                 | ||_ .|_||_| o  |" + NEW_LINE + 
		    "                 |_ __ __ __ __ __|";
	
	private static final String JSON_EXTENSION = "json";
	
	// TODO docs: note in javadoc to type exit
	public static void main(String[] args) {
				
		// The first arg is true/false whether to enter encrypted properties
		boolean enterEncryptedProps = Boolean.parseBoolean(args[0]);
		
		if (enterEncryptedProps) {
			enterEncryptedProps(args);
		} else {
			runRPA(args);
		}
	}
		
	private static void enterEncryptedProps(String[] args) {
		
		// Scanner for user input
		Scanner scanner = new Scanner(System.in);
		
		try {
			Cryptographer.main(args);
			LOGGER.info("Exiting the application.");
		} catch (Exception e) {
			LOGGER.error("Fatal error.", e);
		} finally {
			scanner.close();
		}
	}
	
	private static void runRPA(String[] args) {
				
		// Scanner for user input
		Scanner scanner = new Scanner(System.in);
				
		// All the jobs that are scheduled in this main method
		List<JobKey> allJobs = new ArrayList<>();
				
		try {
			
			// Get the scheduler now, which also starts it
			LOGGER.info(NEW_LINE);
			Scheduler scheduler = SchedulerUtil.getScheduler();

			// Display the intro
			LOGGER.info(RPA_INTRO);
			
			// Convert the args to a list to make for easier checking
			List<String> argsList = Arrays.asList(args);
			
			// Rest of args are the json files we want to trigger right away
			// Store the keys of jobs we want to trigger right away
			List<JobKey> triggerNowJobKeys = new ArrayList<>();
			
			// Parse through every json file in the json directory
			LOGGER.info("Using the rpa-config directory " + RPAProps.getInstance().getProperty(RPAProps.RPA_CONFIG_DIRECTORY_KEY) + ".");
			File jsonDirectory = new File(RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY));
			for (File file : jsonDirectory.listFiles()) {
				if (file.isFile()) {
					String fileName = file.getName();
					if (FilenameUtils.getExtension(fileName).equals(JSON_EXTENSION)) {
						LOGGER.info("Parsing " + fileName + ".");
						JobKey jobKey = JobConfigParser.parse(fileName, false);
						allJobs.add(jobKey);
						
						// If specified, add the job key to the list of jobs to trigger now
						if (argsList.contains(FilenameUtils.getBaseName(fileName))) {
							triggerNowJobKeys.add(jobKey);
						}
					}
				}
			}
			
			// Trigger specified jobs now
			Object monitor = new Object();
			for (JobKey jobKey : triggerNowJobKeys) {
				LOGGER.info("Triggering " + jobKey.getName() + ".");
				synchronized (monitor) {
					monitor.wait(1000L); // Stagger them a bit
				}
				scheduler.triggerJob(jobKey);
			}
			
			// Exit logic
			LOGGER.info("Enter 'exit' at any time to shutdown the application.");
			String command = "";
			while (!command.equals("exit")) {
				command = scanner.nextLine();
			}
			LOGGER.info("Received request to shutdown the application.");
		} catch (Exception e) {
			LOGGER.error("Fatal error.", e);
		} finally {
			scanner.close();			
			try {
				gracefullyTerminateScheduler(allJobs);
			} catch (SchedulerException e) {
				LOGGER.error("Unexpected exception occured terminating the scheduler.", e);
			}
			JedisStore.getInstance().destroy();	
		}
		LOGGER.info("Exiting the application.");
	}
	
	private static void gracefullyTerminateScheduler(List<JobKey> allJobs) throws SchedulerException {
		Scheduler scheduler = SchedulerUtil.getScheduler();
		try {
			
			// Get the list of currently executing jobs
			List<JobKey> currentlyExecutingJobs = new ArrayList<>();
			for (JobExecutionContext context : scheduler.getCurrentlyExecutingJobs()) {
				currentlyExecutingJobs.add(context.getJobDetail().getKey());
			}
			
			/*
			 * Interrupt any top-level jobs that are still executing. Have to do it this way
			 * rather than shutting down all currently executing jobs, because top-level
			 * jobs may create sub-jobs and have their own shutdown logic to terminate
			 * sub-jobs.
			 */
			for (JobKey jobKey : allJobs) {
				if (currentlyExecutingJobs.contains(jobKey)) {
			        LOGGER.info("Terminating the job " + jobKey.getName() + ".");
			        scheduler.interrupt(jobKey);
				}
			}
			
			// If termination request was made successfully,
			// then allow jobs to finish terminating
			SchedulerUtil.shutdownScheduler(true);
		} catch (SchedulerException e) {

			LOGGER.warn("Unable to gracefully terminate jobs.", e);
			SchedulerUtil.shutdownScheduler(false);
		}
	}
}