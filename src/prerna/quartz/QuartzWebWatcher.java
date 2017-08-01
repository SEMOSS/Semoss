package prerna.quartz;

import static org.quartz.JobBuilder.newJob;
import static org.quartz.impl.StdSchedulerFactory.getDefaultScheduler;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import prerna.util.AbstractFileWatcher;
import prerna.util.DIHelper;

public class QuartzWebWatcher extends AbstractFileWatcher {

	private static final String ACTION_TYPE_EMAIL = "email";
	private static final String SEMOSS_JOB_GROUP = "semossJobs";
	
	protected static final Logger LOGGER = LogManager.getLogger(QuartzWebWatcher.class.getName());

	@Override
	public void loadFirst() {
		String[] fileNames = new File(folderToWatch).list();
		for (String fileName : fileNames) {
			if (needToProcess(fileName)) {
				process(fileName);
			}
		}		
	}

	@Override
	public void process(String fileName) {
		
		// Get the json string from a file
		String jsonString;
		try {
			jsonString = new String(Files.readAllBytes(Paths.get(folderToWatch + "/" + fileName)), "UTF-8");
		} catch (IOException e) {
			e.printStackTrace();
			LOGGER.error("Unable to read the file " + fileName);
			return;
		}
		
		// Parse the json string into an object
		JSONObject json;
		try {
			json = new JSONObject(jsonString);
		} catch (JSONException e) {
			e.printStackTrace();
			LOGGER.error("Unable to parse json object from " + fileName);
			return;
		}
		
		// Add data from the json object into the job data map
		try {
			JSONObject job = json.getJSONObject("schedule").getJSONObject("job");
			JSONObject values = job.getJSONObject("values");
			JSONObject action = job.getJSONObject("action");
			JSONObject trigger = json.getJSONObject("schedule").getJSONObject("trigger");

			String rdbmsId = values.getString("rdbms-id");
			String engine = values.getString("engine");
			String valuePkql = values.getString("value-pkql");
			
			// Setup the job chain
			JobDataMap jobChainMap = new JobDataMap();
			List<Class<? extends Job>> jobSequence = new ArrayList<Class<? extends Job>>();
			
			// WaitForEngineToLoadJob
			jobSequence.add(WaitForEngineToLoadJob.class);
			jobChainMap.put(WaitForEngineToLoadJob.IN_ENGINE_NAME_KEY, engine);
			
			// GetInsightJob
			jobSequence.add(GetInsightJob.class);
			jobChainMap.put(GetInsightJob.IN_RDBMS_ID, rdbmsId);
			
			// GetDataFromInsightJob
			jobSequence.add(GetDataFromInsightJob.class);
		
			// PrintDataToConsoleJob
			jobSequence.add(PrintDataToConsoleJob.class);
			
			// TODO make the comparison
			
			// TODO IfJob <- action job needs to be passed in as well
					
			// Add the sequence
			jobChainMap.put(JobChain.IN_SEQUENCE, jobSequence);
			
			// The job chain itself
			JobDetail jobChain = newJob(JobChain.class).withIdentity("jobChain", SEMOSS_JOB_GROUP).usingJobData(jobChainMap).build();
	
			// Trigger the job chain
			try {
				Scheduler scheduler = getDefaultScheduler();
				scheduler.start();
				scheduler.addJob(jobChain, true, true);
				scheduler.triggerJob(jobChain.getKey());
			} catch (SchedulerException e) {
				e.printStackTrace();
			}
			
			String actionType = action.getString("actiontype");
			System.out.println(actionType);
									
			if (actionType.equals(ACTION_TYPE_EMAIL)) {
				JSONObject email = action.getJSONObject(ACTION_TYPE_EMAIL);
				String emailAddress = email.getString("email-address");
				System.out.println(emailAddress);
			}
			String frequency = trigger.getString("frequency");
			System.out.println(frequency);
			
		} catch (JSONException e) {
			e.printStackTrace();
			LOGGER.error("Unable to find necessary key in " + fileName);
			return;
		}
				

		// Construct the job chain
		
		// Create a trigger using json data
		
		// Schedule the job chain using the trigger
	}
	
	private boolean needToProcess(String fileName) {
		return fileName.endsWith(extension);
	}

	@Override
	public void run() {
		
		// TODO start up the scheduler
		
		loadFirst();
		try {
			WatchService watcher = FileSystems.getDefault().newWatchService();
			String baseFolder = DIHelper.getInstance().getProperty("BaseFolder");

			// Path dir2Watch = Paths.get(baseFolder + "/" + folderToWatch);

			Path dir2Watch = Paths.get(folderToWatch);

			WatchKey key = dir2Watch.register(watcher, StandardWatchEventKinds.ENTRY_CREATE,
					StandardWatchEventKinds.ENTRY_MODIFY);
			while (true) {
				// WatchKey key2 = watcher.poll(1, TimeUnit.MINUTES);
				WatchKey key2 = watcher.take();

				for (WatchEvent<?> event : key2.pollEvents()) {
					WatchEvent.Kind kind = event.kind();
					if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
						String newFile = event.context() + "";
						if (newFile.endsWith(extension)) {
							Thread.sleep(2000);
							try {
								process(newFile);

							} catch (RuntimeException ex) {
								ex.printStackTrace();
							}
						} else
							logger.info("Ignoring File " + newFile);
					}
				}
				key2.reset();
			}
		} catch (RuntimeException ex) {
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (InterruptedException ex) {
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		} catch (IOException ex) {
			logger.debug(ex);
			// do nothing - I will be working it in the process block
		}
	}

}
