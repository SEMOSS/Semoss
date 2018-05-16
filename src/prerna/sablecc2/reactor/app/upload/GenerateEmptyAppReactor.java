package prerna.sablecc2.reactor.app.upload;

import java.io.File;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.impl.app.AppEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.solr.SolrUtility;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GenerateEmptyAppReactor extends AbstractReactor {

	private static final String CLASS_NAME = GenerateEmptyAppReactor.class.getName();

	/*
	 * This class is used to construct an empty app
	 * This app contains no data (no data file or OWL)
	 * This app only contains insights
	 * The idea being that the insights are parameterized and can be applied to various data sources
	 */

	public GenerateEmptyAppReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.APP.getKey()};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		this.organizeKeys();
		String appName = this.keyValue.get(this.keysToGet[0]);
		
		// start by validation
		logger.info("Start validating app");
		try {
			UploadUtilities.validateApp(appName);
		} catch (IOException e) {
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done validating app");
		
		logger.info("Starting app creation");

		/*
		 * Things we need to do
		 * 1) make directory
		 * 2) make insights database
		 * 3) make special smss
		 * 4) load into solr
		 */

		logger.info("Start generating app folder");
		UploadUtilities.generateAppFolder(appName);
		logger.info("Done generating app folder");

		logger.info("Start generating insights database");
		IEngine insightDb = UploadUtilities.generateInsightsDatabase(appName);
		logger.info("Done generating insights database");

		// add to DIHelper so we dont auto load with the file watcher
		File tempAppSmss = null;
		logger.info("Start generating temp smss");
		try {
			tempAppSmss = UploadUtilities.createTemporaryAppSmss(appName);
			DIHelper.getInstance().getCoreProp().setProperty(appName + "_" + Constants.STORE, tempAppSmss.getAbsolutePath());
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		logger.info("Done generating temp smss");

		logger.info("Start loading into solr");
		try {
			SolrUtility.addAppToSolr(appName);
		} catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e) {
			e.printStackTrace();
		}
		logger.info("Done loading into solr");

		AppEngine appEng = new AppEngine();
		appEng.setInsightDatabase(insightDb);
		// only at end do we add to DIHelper
		DIHelper.getInstance().setLocalProperty(appName, appEng);
		String appNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
		appNames = appNames + ";" + appName;
		DIHelper.getInstance().setLocalProperty(Constants.ENGINES, appNames);
		
		// and rename .temp to .smss
		File smssFile = new File(tempAppSmss.getAbsolutePath().replace(".temp", ".smss"));
		try {
			FileUtils.copyFile(tempAppSmss, smssFile);
		} catch (IOException e) {
			e.printStackTrace();
		}
		tempAppSmss.delete();
		
		return new NounMetadata(true, PixelDataType.BOOLEAN);
	}
}
