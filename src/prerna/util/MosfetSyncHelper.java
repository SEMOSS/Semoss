package prerna.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.auth.utils.SecurityInsightUtils;
import prerna.engine.api.IEngine;
import prerna.engine.impl.InsightAdministrator;
import prerna.engine.impl.SmssUtilities;
import prerna.om.Insight;
import prerna.om.MosfetFile;

public class MosfetSyncHelper {

	// get the directory separator
	private static final String DIR_SEPARATOR = java.nio.file.FileSystems.getDefault().getSeparator();

	// ADDED
	public static final String ADD = "ADD";
	// MODIFIED
	public static final String MOD = "MOD";
	// DELETE
	public static final String DEL = "DEL";
	// RENAMED
	public static final String REN = "REN";

	public static final String ENGINE_ID_KEY = "engineId";
	public static final String RDBMS_ID_KEY = "rdbmsId";
	public static final String INSIGHT_NAME_KEY = "insightName";
	public static final String LAYOUT_KEY = "layout";
	public static final String RECIPE_KEY = "recipe";
	public static final String HIDDEN_KEY = "hidden";

	private MosfetSyncHelper() {

	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * This section for synchronizing files
	 */

	public static void synchronizeInsightChanges(Map<String, List<String>> filesChanged, Logger logger) {
		// process add
		if(filesChanged.containsKey(ADD)) {
			processAddedFiles(filesChanged.get(ADD), logger);
		}

		// process mod
		if(filesChanged.containsKey(MOD)) {
			processModifiedFiles(filesChanged.get(MOD), logger);
		}

		// process delete
		if(filesChanged.containsKey(DEL)) {
			processDelete(filesChanged.get(DEL), logger);
		}
	}

	private static void processAddedFiles(List<String> list, Logger logger) {
		for(String fileLocation : list) {
			File file = new File(Utility.normalizePath(fileLocation));
			MosfetFile mosfetFile;
			try {
				mosfetFile = MosfetFile.generateFromFile(file);
			} catch (IOException e) {
				outputError(logger, "MOSFET file is not in valid JSON format");
				logger.error(Constants.STACKTRACE, e);
				continue;
			}
			processAddedFile(mosfetFile, logger);
		}
	}

	private static void processAddedFile(MosfetFile mosfet, Logger logger) {
		String appId = mosfet.getEngineId();
		String id = mosfet.getRdbmsId();

		// need to add the insight in the rdbms engine
		IEngine engine = Utility.getEngine(appId);
		// we want to make sure the file isn't added because we made the insight
		// and is in fact a new one made by another collaborator
		Vector<Insight> ins = engine.getInsight(id);
		if(ins == null || ins.isEmpty() || (ins.size() == 1 && ins.get(0) == null) ) {
			logger.info("Start processing new mosfet file");
			addInsightToEngineRdbms(engine, mosfet);
			logger.info("Done processing mosfet file");
		}
	}

	private static void processModifiedFiles(List<String> list, Logger logger) {
		for(String fileLocation : list) {
			File file = new File(Utility.normalizePath(fileLocation));
			MosfetFile mosfetFile;
			try {
				mosfetFile = MosfetFile.generateFromFile(file);
			} catch (IOException e) {
				outputError(logger, "MOSFET file is not in valid JSON format");
				logger.error(Constants.STACKTRACE, e);
				continue;
			}
			processModifiedFiles(mosfetFile, logger);
		}
	}

	private static void processModifiedFiles(MosfetFile mosfet, Logger logger) {
		logger.info("Start editing existing mosfet file");
		updateInsightInEngineRdbms(mosfet);
		logger.info("Done processing mosfet file");
	}

	private static void processDelete(List<String> list, Logger logger) {
		for(String fileLocation : list) {
			File file = new File(Utility.normalizePath(fileLocation));
			MosfetFile mosfetFile;
			try {
				mosfetFile = MosfetFile.generateFromFile(file);
			} catch (IOException e) {
				outputError(logger, "MOSFET file is not in valid JSON format");
				logger.error(Constants.STACKTRACE, e);
				continue;
			}
			logger.info("Start deleting mosfet file");
			deleteInsightFromEngineRdbms(mosfetFile);
			logger.info("Done deleting mosfet file");
		}
	}

	public static void addInsightToEngineRdbms(IEngine engine, MosfetFile mosfet) {
		String appId = mosfet.getEngineId();
		String id = mosfet.getRdbmsId();
		String insightName = mosfet.getInsightName();
		String layout = mosfet.getLayout();
		List<String> recipe = mosfet.getRecipe();
		boolean hidden = mosfet.isHidden();
		boolean cacheable = Utility.getApplicationCacheInsight();

		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		admin.addInsight(id, insightName, layout, recipe, hidden, cacheable);
		SecurityInsightUtils.addInsight(appId, id, insightName, false, cacheable, layout, recipe);

		// also sync the metadata
		String description = mosfet.getDescription();
		if(description != null) {
			admin.updateInsightDescription(id, description);
			SecurityInsightUtils.updateInsightDescription(appId, id, description);
		}
		String[] tags = mosfet.getTags();
		if(tags != null && tags.length > 0) {
			admin.updateInsightTags(id, tags);
			SecurityInsightUtils.updateInsightTags(id, id, tags);
		}
	}

	public static void updateInsightInEngineRdbms(MosfetFile mosfet) {
		String appId = mosfet.getEngineId();
		String id = mosfet.getRdbmsId();
		String insightName = mosfet.getInsightName();
		String layout = mosfet.getLayout();
		List<String> recipe = mosfet.getRecipe();
		boolean hidden = mosfet.isHidden();

		IEngine engine = Utility.getEngine(appId);

		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		// just put the recipe into an array
		admin.updateInsight(id, insightName, layout, recipe, hidden);
		SecurityInsightUtils.updateInsight(appId, id, insightName, false, layout, recipe);

		// also sync the metadata
		String description = mosfet.getDescription();
		if(description != null) {
			admin.updateInsightDescription(id, description);
			SecurityInsightUtils.updateInsightDescription(appId, id, description);
		}
		String[] tags = mosfet.getTags();
		if(tags != null && tags.length > 0) {
			admin.updateInsightTags(id, tags);
			SecurityInsightUtils.updateInsightTags(id, id, tags);
		}
	}

	public static void deleteInsightFromEngineRdbms(MosfetFile mosfet) {
		String appId = mosfet.getEngineId();
		String id = mosfet.getRdbmsId();

		IEngine engine = Utility.getEngine(appId);
		InsightAdministrator admin = new InsightAdministrator(engine.getInsightDatabase());
		admin.dropInsight(id);

		SecurityInsightUtils.deleteInsight(appId, id);
	}

	private static void outputError(Logger logger, String errorMessage) {
		if(logger != null) {
			logger.info("ERROR!!! " + errorMessage);
		}
	}

	////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////////////////////////////

	/*
	 * These methods only update the mosfet file itself and not the metadata (security db or insights db)
	 */

	/**
	 * Get the insight name for the input mosfet file
	 * @param mosfetFile
	 * @return
	 * @throws IOException 
	 */
	public static String getInsightName(File mosfetFile) throws IOException {
		MosfetFile mosfet = MosfetFile.generateFromFile(mosfetFile);
		return mosfet.getInsightName();
	}

	/**
	 * Only generate the mosfet file
	 * @param appId
	 * @param appName
	 * @param rdbmsId
	 * @param insightName
	 * @param layout
	 * @param recipe
	 * @param hidden
	 * @return
	 * @throws IOException 
	 */
	public static File makeMosfitFile(String appId, String appName, String rdbmsId, String insightName, 
			String layout, List<String> recipe, boolean hidden) throws IOException {
		MosfetFile mosfet = new MosfetFile();
		mosfet.setEngineId(appId);
		mosfet.setRdbmsId(rdbmsId);
		mosfet.setInsightName(insightName);
		mosfet.setLayout(layout);
		mosfet.setRecipe(recipe);
		mosfet.setHidden(hidden);

		String mosfetPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)
				+ DIR_SEPARATOR + "version" 
				+ DIR_SEPARATOR + rdbmsId;

		mosfet.write(mosfetPath, false);

		return new File(Utility.normalizePath(mosfetPath) + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);
	}

	/**
	 * Only generate the mosfet file
	 * @param appId
	 * @param appName
	 * @param rdbmsId
	 * @param insightName
	 * @param layout
	 * @param recipe
	 * @param hidden
	 * @param description
	 * @param tags
	 * @return
	 * @throws IOException 
	 */
	public static File makeMosfitFile(String appId, String appName, String rdbmsId, String insightName, 
			String layout, List<String> recipe, boolean hidden, String description, List<String> tags) throws IOException {
		return makeMosfitFile(appId, appName, rdbmsId, insightName, layout, recipe, hidden, description, tags, false);
	}
	
	/**
	 * Only generate the mosfet file
	 * @param appId
	 * @param appName
	 * @param rdbmsId
	 * @param insightName
	 * @param layout
	 * @param recipe
	 * @param hidden
	 * @param description
	 * @param tags
	 * @param forceDelete
	 * @return
	 * @throws IOException 
	 */
	public static File makeMosfitFile(String appId, String appName, String rdbmsId, String insightName, 
			String layout, List<String> recipe, boolean hidden, String description, List<String> tags, boolean forceDelete) throws IOException {
		MosfetFile mosfet = new MosfetFile();
		mosfet.setEngineId(appId);
		mosfet.setRdbmsId(rdbmsId);
		mosfet.setInsightName(insightName);
		mosfet.setLayout(layout);
		mosfet.setRecipe(recipe);
		mosfet.setHidden(hidden);
		if(description != null) {
			mosfet.setDescription(description);
		}
		if(tags != null && !tags.isEmpty()) {
			mosfet.setTags(tags.toArray(new String[tags.size()]));
		}

		String mosfetPath = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER)
				+ DIR_SEPARATOR + "db"
				+ DIR_SEPARATOR + SmssUtilities.getUniqueName(appName, appId)
				+ DIR_SEPARATOR + "version" 
				+ DIR_SEPARATOR + rdbmsId;

		mosfet.write(mosfetPath, forceDelete);
		return new File(mosfetPath + DIR_SEPARATOR + MosfetFile.RECIPE_FILE);
	}

	/**
	 * Only update the mosfet file
	 * @param mosfetFile
	 * @param appId
	 * @param appName
	 * @param rdbmsId
	 * @param insightName
	 * @param layout
	 * @param imageFileName
	 * @param recipe
	 * @param hidden
	 * @return
	 * @throws IOException 
	 */
	public static File updateMosfitFile(File mosfetFile, String appId, String appName, String rdbmsId, String insightName, 
			String layout, String imageFileName, List<String> recipe, boolean hidden) throws IOException {
		MosfetFile mosfet = new MosfetFile();
		mosfet.setEngineId(appId);
		mosfet.setRdbmsId(rdbmsId);
		mosfet.setInsightName(insightName);
		mosfet.setLayout(layout);
		mosfet.setRecipe(recipe);
		mosfet.setHidden(hidden);

		mosfet.write(mosfetFile.getParentFile().getAbsolutePath(), true);
		return mosfetFile;
	}

	/**
	 * Only update the mosfet file
	 * @param mosfetFile
	 * @param appId
	 * @param appName
	 * @param rdbmsId
	 * @param insightName
	 * @param layout
	 * @param imageFileName
	 * @param recipe
	 * @param hidden
	 * @param description
	 * @param tags
	 * @return
	 * @throws IOException 
	 */
	public static File updateMosfitFile(File mosfetFile, String appId, String appName, String rdbmsId, String insightName,
			String layout, String imageFileName, List<String> recipe, boolean hidden, String description, List<String> tags) throws IOException {
		MosfetFile mosfet = new MosfetFile();
		mosfet.setEngineId(appId);
		mosfet.setRdbmsId(rdbmsId);
		mosfet.setInsightName(insightName);
		mosfet.setLayout(layout);
		mosfet.setRecipe(recipe);
		mosfet.setHidden(hidden);
		if(description != null) {
			mosfet.setDescription(description);
		}
		if(tags != null && !tags.isEmpty()) {
			mosfet.setTags(tags.toArray(new String[tags.size()]));
		}

		mosfet.write(mosfetFile.getParentFile().getAbsolutePath(), true);
		return mosfetFile;
	}

	/**
	 * Only update the mosfet file insight name
	 * @param mosfetFile
	 * @param newInsightName
	 * @return
	 * @throws IOException
	 */
	public static File updateMosfitFileInsightName(File mosfetFile, String newInsightName) throws IOException {
		MosfetFile mosfet = MosfetFile.generateFromFile(mosfetFile);
		mosfet.setInsightName(newInsightName);
		mosfet.write(mosfetFile.getParentFile().getAbsolutePath(), true);
		return mosfetFile;
	}

}
