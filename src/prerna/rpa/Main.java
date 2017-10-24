package prerna.rpa;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobKey;
import org.quartz.Scheduler;
import prerna.rpa.config.JobConfigParser;
import prerna.rpa.quartz.SchedulerUtil;
import prerna.rpa.security.Cryptographer;

public class Main {
	
	private static final Logger LOGGER = LogManager.getLogger(Main.class.getName());
	
	private static final String NEW_LINE = System.getProperty("line.separator");
	
	private static final String RPA_INTRO = 
			NEW_LINE +
			" __ __ __ __ __ _" + NEW_LINE + 
			"|                | SEMOSS RPA 2.0" + NEW_LINE + 
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
	
	public static void main(String[] args) {
		try {
			// args are the json files we want to trigger right away
			// Convert the args to a list to make for easier checking
			List<String> argsList = Arrays.asList(args);
			
			// Store the keys of jobs we want to trigger right away
			List<JobKey> triggerNowJobKeys = new ArrayList<JobKey>();
			
			// The first arg is true/false whether to enter encrypted properties
			boolean enterEncryptedProps = Boolean.parseBoolean(argsList.get(0));
			if (enterEncryptedProps) {
				Cryptographer.main(args);
				
				// End the program
				return;
			}
			
			LOGGER.info(NEW_LINE);
			
			// Get the scheduler now, which also starts it
			Scheduler scheduler = SchedulerUtil.getScheduler();
			
			// Display the intro
			LOGGER.info(RPA_INTRO);
			
			// Parse through every json file in the json directory
			LOGGER.info("Using the rpa-config directory " + RPAProps.getInstance().getProperty(RPAProps.RPA_CONFIG_DIRECTORY_KEY) + ".");
			File jsonDirectory = new File(RPAProps.getInstance().getProperty(RPAProps.JSON_DIRECTORY_KEY));
			for (File file : jsonDirectory.listFiles()) {
				if (file.isFile()) {
					String fileName = file.getName();
					if (FilenameUtils.getExtension(fileName).equals(JSON_EXTENSION)) {
						LOGGER.info("Parsing " + fileName + ".");
						JobKey jobKey = JobConfigParser.parse(fileName, false);
						
						// If specified, add the job key to the list of jobs to trigger now
						if (argsList.contains(FilenameUtils.getBaseName(fileName))) {
							triggerNowJobKeys.add(jobKey);
						}
					}
				}
			}
			
			// Trigger specified jobs now
			for (JobKey jobKey : triggerNowJobKeys) {
				LOGGER.info("Triggering " + jobKey.getName() + ".");
				scheduler.triggerJob(jobKey);
			}
		} catch (Exception e) {
			LOGGER.error("Fatal error.", e);
		}
	}
	
}