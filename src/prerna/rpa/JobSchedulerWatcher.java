package prerna.rpa;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.quartz.JobKey;

import prerna.rpa.config.JobConfigParser;
import prerna.rpa.quartz.SchedulerUtil;
import prerna.util.AbstractFileWatcher;

public class JobSchedulerWatcher extends AbstractFileWatcher {

	private static final Logger LOGGER = LogManager.getLogger(JobSchedulerWatcher.class.getName());
	
	private static final String JSON_EXTENSION = "json";
	
	@Override
	public void loadFirst() {		
		File jsonDirectory = new File(folderToWatch);
		String[] fileNames = jsonDirectory.list();
		if (fileNames != null) {
			for (String fileName : fileNames) {
				process(fileName);
			}
		}
	}

	@Override
	public void process(String fileName) {
		try {
			if (FilenameUtils.getExtension(fileName).equals(JSON_EXTENSION)) {
				LOGGER.info("Parsing " + fileName + ".");
				JobKey jobKey = JobConfigParser.parse(fileName, false);
				try {
					if (Arrays.asList(RPAProps.getInstance().getProperty(RPAProps.TRIGGER_NOW).split(";")).contains(fileName)) {
						LOGGER.info("Triggering " + jobKey.getName() + ".");
						SchedulerUtil.getScheduler().triggerJob(jobKey);
					}
				} catch (Exception e) {
					LOGGER.error("Failed to trigger job from " + fileName + ".", e);
				}
			}
		} catch (Exception e) {
			LOGGER.error("Failed to parse job from " + fileName + ".", e);
		}
	}

	@Override
	public void run() {
		LOGGER.info("Starting JobSchedulerWatcher thread.");
		loadFirst();
		super.run();
	}
}
