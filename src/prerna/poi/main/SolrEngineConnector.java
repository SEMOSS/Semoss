package prerna.poi.main;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import prerna.cache.ICache;
import prerna.engine.api.IEngine;
import prerna.engine.impl.solr.SolrEngine;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class SolrEngineConnector extends AbstractEngineCreator {

	/**
	 * Generate a new engine based on an existing solr core
	 * @param solrUri
	 * @param solrCoreName
	 * @return
	 * @throws IOException 
	 */
	public IEngine processExistingSolrConnection(String dbName, String solrURL, String solrCoreName) throws IOException {
		/*
		 * Processing steps
		 * 1) create the engine folder (using db name)
		 * 2) crate a new SolrEngine
		 * 3) create the owl file
		 * 4) create the default insights engine file
		 * 5) create the temp smss file
		 * 6) add to local master
		 * 7) delete temp smss
		 * 8) make real smss
		 */
		
		// 1) create the engine folder
		String baseDirectory = DIHelper.getInstance().getProperty("BaseFolder");
		String engineDirectoryName = baseDirectory + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + dbName;
		File engineDirectory = new File(engineDirectoryName);
		if(engineDirectory.exists()) {
			throw new IOException("Engine directory already exists!  Need admin privelages to remove the folder before uploading an engine with the specified name.");
		}
		FileUtils.forceMkdir(engineDirectory);
		
		// 2) create a new solr engine
		this.engine = new SolrEngine(solrURL, solrCoreName);
		this.engine.setEngineId(dbName);
		
		// 3) create the owler
		this.owlFile = engineDirectoryName + System.getProperty("file.separator") + dbName + "_OWL.OWL";
		this.owler = SolrEngine.getSolrEngineOWLER(owlFile, solrURL, solrCoreName);
		createBaseRelations();
		
		// 4) create the base insights 
		// TODO: i have this in future code.. right now, we need to make a question file
		this.engine.setInsightDatabase(createNewInsightsDatabase(dbName, this.engine.getEngineId()));
		
		//) 5-8
		String tempSmssLocation = baseDirectory + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + dbName + ".temp"; 
		String smssLocation = baseDirectory + System.getProperty("file.separator") + "db" + System.getProperty("file.separator") + dbName + ".smss"; 

		File tempFile = null;
		File smssFile = null;
		
		try {
			tempFile = wirteSmssFile(dbName, tempSmssLocation, solrURL, solrCoreName);
			
			DIHelper.getInstance().getCoreProp().setProperty(dbName + "_" + Constants.STORE, tempSmssLocation);
			Utility.synchronizeEngineMetadata(dbName); // replacing this for engine
			// only after all of this is good, should we add it to DIHelper
			DIHelper.getInstance().setLocalProperty(dbName, this.engine);
			String engineNames = (String) DIHelper.getInstance().getLocalProp(Constants.ENGINES);
			engineNames = engineNames + ";" + dbName;
			DIHelper.getInstance().setLocalProperty(Constants.ENGINES, engineNames);
			
			// but we need to change to the true smss
			smssFile = wirteSmssFile(dbName, smssLocation, solrURL, solrCoreName);
			DIHelper.getInstance().getCoreProp().setProperty(dbName + "_" + Constants.STORE, smssLocation);
			
			// set the correct prop file
			// set the owl
			this.engine.setPropFile(smssLocation);
			this.engine.setOWL(this.owler.getOwlPath());
		} catch (Exception e) {
			if(smssFile != null) {
				ICache.deleteFile(smssFile);
			}
			throw new IOException(e.getMessage());
		} finally {
			if(tempFile != null) {
				ICache.deleteFile(tempFile);
			}
		}
		
		return this.engine;
	}
	
	private File wirteSmssFile(String dbName, String smssLocation, String solrURL, String solrCoreName) throws IOException {
		// changing to params
		
		//String owlPropValue = "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + dbName + "_OWL.OWL";
		String owlPropValue = "db" + System.getProperty("file.separator") + "@engine@" + System.getProperty("file.separator") + dbName + "_OWL.OWL";
		
		//String insightsRdbmsPropValue = "db" + System.getProperty("file.separator") + dbName + System.getProperty("file.separator") + "insights_database";
		String insightsRdbmsPropValue = "db" + System.getProperty("file.separator") + "@engine@" + System.getProperty("file.separator") + "insights_database";

		File f = new File(smssLocation);
		FileWriter pw = null;
		try {
			pw = new FileWriter(f);
			pw.write("Base Properties\n");
			pw.write(Constants.ENGINE + "\t" + dbName + "\n");
			pw.write(Constants.ENGINE_TYPE + "\tprerna.engine.impl.solr.SolrEngine\n");
			pw.write(Constants.OWL + "\t" + owlPropValue + "\n");
			
			pw.write(Constants.RDBMS_INSIGHTS + "\t" + insightsRdbmsPropValue + "\n");
			pw.write("\n");
			pw.write(Constants.SOLR_URL + "\t" + solrURL + "\n");
			pw.write(Constants.SOLR_CORE_NAME + "\t" + solrCoreName + "\n");
		} catch (IOException e) {
			e.printStackTrace();
			throw new IOException("Error with creating semoss property file for engine");
		} finally {
			if(pw != null) {
				try {
					pw.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return f;
	}
}
