package prerna.ui.components.specific.tap.genesisdeployment;

import java.io.IOException;
import java.io.PrintWriter;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.ds.h2.H2Frame;
import prerna.engine.api.IEngine;
import prerna.engine.impl.rdf.BigDataEngine;
import prerna.test.TestUtilityMethods;
import prerna.ui.components.playsheets.TablePlaySheet;
import prerna.ui.components.playsheets.datamakers.DataMakerComponent;
import prerna.util.DIHelper;

public class MhsGenesisSystemDeploymentSavingsPlaySheet extends TablePlaySheet { 

	private static final Logger LOGGER = LogManager.getLogger(MhsGenesisDeploymentSavingsProcessor.class.getName());

	// store the system deployment savings over time
	private H2Frame systemDeploymentSavings;

	// frame to keep track of waves to sites to systems to fys containing inflated costs
	protected H2Frame mainSustainmentFrame;
	// frame to keep track of systems to sits to fys containing the inflated costs
	// only for systems which have site level cost information
	private H2Frame systemSiteSustainmentFrame;

	// need to get the wave start and end
	private Map<String, String[]> waveStartEndDate;

	private int numColumns;
	private double percentRealized;

	public MhsGenesisSystemDeploymentSavingsPlaySheet(MhsGenesisDeploymentSavingsProcessor processor) {
		processor.runSupportQueries();

		// frame to keep track of waves to sites to systems to fys containing inflated costs
		this.mainSustainmentFrame = processor.mainSustainmentFrame;
		// frame to keep track of systems to sits to fys containing the inflated costs
		// only for systems which have site level cost information
		this.systemSiteSustainmentFrame = processor.systemSiteSustainmentFrame;

		// need to get the wave start and end
		this.waveStartEndDate = processor.waveStartEndDate;

		this.numColumns = processor.numColumns;
		this.percentRealized = processor.percentRealized;
	}

	@Override
	public void processDataMakerComponent(DataMakerComponent component) {
		// generate the necessary data
		generateSystemDeploymentSavings();
	}


	@Override
	public Hashtable getDataMakerOutput(String... selectors) {
		//TODO: finish this
		//TODO: finish this
		//TODO: finish this
		//TODO: finish this
		//TODO: finish this
		Hashtable returnHash = new Hashtable();
		return returnHash;
	}

	/**
	 * Calculate the cost of system deployment savings
	 */
	public void generateSystemDeploymentSavings() {
		// okay, create the frame that will hold
		// the information regarding the system
		// deployment savings
		// so each row represents a system and its savings
		// across each FY
		String[] headers = new String[numColumns+1];
		Map<String, String> dataTypes = new Hashtable<String, String>();
		headers[0] = "System";
		dataTypes.put("System", "String");
		for(int i = 0; i < numColumns; i++) {
			headers[i+1] = "FY" + (15 + i);
			dataTypes.put(headers[i+1], "Number");
		}
		this.systemDeploymentSavings = new H2Frame(headers);
		this.systemDeploymentSavings.mergeDataTypeMap(dataTypes);

		// just going to go through and add all the different type of savings
		addLocallyDeployedSystemSavings();
		addGloballyDeployedSystemSavings();
		addSiteSpecificSystemSavings();
		addFixedSustainmentCostForAllLocallyDeployedSystems();
		
		// this will add the column and row totals
		MhsGenesisDeploymentSavingsProcessor.calculateColAndRowTotals(this.systemDeploymentSavings);
		headers = this.systemDeploymentSavings.getColumnHeaders();

		// iterate through the results for testing
		// iterate through the results for testing
		// iterate through the results for testing
		// iterate through the results for testing
		// iterate through the results for testing
		LOGGER.info("Testing data...");
		Iterator<Object[]> it = this.systemDeploymentSavings.iterator();
		System.out.println(">>> " + Arrays.toString( headers ) );
		try{
			PrintWriter writer = new PrintWriter("C:\\Users\\mahkhalil\\Desktop\\Datasets\\SystemDeploymentSavings.csv", "UTF-8");
			for(Object val : headers) {
				writer.print(val + ",");
			}
			writer.print("\n");
			while(it.hasNext()) {
				Object[] values = it.next();
				for(Object val : values) {
					writer.print(val + ",");
				}
				writer.print("\n");
				System.out.println(">>> " + Arrays.toString( values ) );
			}
			writer.close();
		} catch (IOException e) {
			// do something
		}
		LOGGER.info("Done iterating through system deployment savings data");
	}
	
	private void addLocallyDeployedSystemSavings() {
		LOGGER.info("START ::: Add locally deployed systems where we do not have site specific costs...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// this section will go ahead and add all the systems
		// which are locally deployed and we do not have site level cost for
		
		// query will be to get all system rows and produce the FY inflated savings / num sites
		// and we will add the appropriate filters such that we are only getting the rows desired
		StringBuilder baseSystemsCostQuery = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			baseSystemsCostQuery.append(", SUM( FY" + (15+i) + " / NUM_SITES ) ");
		}
		baseSystemsCostQuery.append(" FROM ").append(mainSustainmentFrame.getTableName());
		baseSystemsCostQuery.append(" WHERE Central_Deployment='FALSE' AND Site_Specific='FALSE' "); 

		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String[] endTime = startDate[1].split("FY");
			String endYear = "FY" + endTime[1].substring(2);

			// so we have the wave
			// just query to get the cost information
			// for all the systems in the wave
			StringBuilder locallyDeployedSystemCost = new StringBuilder(baseSystemsCostQuery.toString());
			locallyDeployedSystemCost.append(" AND WAVE = '").append(wave).append("' AND ( Last_Wave_For_Site = '-1' OR Last_Wave_For_Site = '" + wave + "') ");
			locallyDeployedSystemCost.append(" GROUP BY SYSTEM;");
			updateSystemCostValues(locallyDeployedSystemCost.toString(), endYear, this.percentRealized);
		}
		LOGGER.info("DONE ::: Finsihed with locally deployed systems where we do not have site specific costs...");
	}
	
	private void addGloballyDeployedSystemSavings() {
		LOGGER.info("START ::: Add globally deployed systems...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// this section will add the globally deployed systems
		// they by definition get added at the last year
		// again, note that we need to account for the additional year for CHCS
		
		StringBuilder globallyDeployedSystemCostSavings = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			globallyDeployedSystemCostSavings.append(", FY" + (15+i) + " / NUM_SITES");
		}
		globallyDeployedSystemCostSavings.append(" FROM ").append(mainSustainmentFrame.getTableName());
		globallyDeployedSystemCostSavings.append(" WHERE Central_Deployment='TRUE';");
		
		// the cost savings for all of these systems will occur on final year of deployment schedule 
		updateSystemCostValues(globallyDeployedSystemCostSavings.toString(), "FY" + (15+numColumns-3), 1.0);
		LOGGER.info("DONE ::: Finsihed with globally deployed systems...");
	}
	
	private void addSiteSpecificSystemSavings() {
		LOGGER.info("START ::: Add systems where we have site specific costs...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// this section will add the site specific sustainment costs
		// base query below is to determine which sites are in each wave
		// as this requires 2 separate queries
		StringBuilder baseSitesInWaveQuery = new StringBuilder("SELECT DISTINCT HostSiteAndFloater FROM ").append(mainSustainmentFrame.getTableName());
		// get all the systems and their cost
		StringBuilder baseSiteSystemSpecificCost = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			baseSiteSystemSpecificCost.append(", FY" + (15+i));
		}
		baseSiteSystemSpecificCost.append(" FROM ").append(systemSiteSustainmentFrame.getTableName());
		
		for(String wave : waveStartEndDate.keySet()) {
			String[] startDate = waveStartEndDate.get(wave);
			String[] endTime = startDate[1].split("FY");
			String endYear = "FY" + endTime[1].substring(2);

			// got to query for the list of sites in this wave
			// this will be used as a filter for the second query to run
			StringBuilder sitesInWaveQuery = new StringBuilder(baseSitesInWaveQuery.toString()).append(" WHERE WAVE = '").append(wave).append("'")
					.append(" AND ( Last_Wave_For_Site = '-1' OR Last_Wave_For_Site = '" + wave + "') ");			
			// create a filter string for the site system specific cost query
			StringBuilder siteSystemFilter = new StringBuilder(" WHERE SITE IN (");
			ResultSet rs = mainSustainmentFrame.execQuery(sitesInWaveQuery.toString());
			try {
				if(rs.next()) {
					siteSystemFilter.append("'").append(rs.getString(1)).append("'");
				}
				while(rs.next()) {
					siteSystemFilter.append(",'").append(rs.getString(1)).append("'");
				}
			} catch (SQLException e) {
				e.printStackTrace();
			}
			siteSystemFilter.append(")");
			
			// so we have the list of sites to filter to in this wave
			// append that to the base site system specific cost query and then update the values
			StringBuilder siteSystemSpecificCost = new StringBuilder(baseSiteSystemSpecificCost.toString()).append(siteSystemFilter.toString());
			// update the base site specific costs
			// this is a different method only because chcs has year of savings pushed by 2 years
			updateSiteSpecificSystemCostValues(siteSystemSpecificCost.toString(), endYear);
		}
		LOGGER.info("DONE ::: Finsihed with systems where we have site specific costs...");
	}
	
	private void addFixedSustainmentCostForAllLocallyDeployedSystems() {
		LOGGER.info("START ::: Add fixed sustainment cost for systems where we do not have site specific costs...");
		// >>>>>>>>>>>>>>>>>>>>>>>
		// add the fixed amount savings
		// get the query to get the sum for all the values group by system
		StringBuilder baseSystemFixedCostQuery = new StringBuilder("SELECT SYSTEM ");
		for(int i = 0; i < numColumns; i++) {
			baseSystemFixedCostQuery.append(", SUM(FY" + (15+i) + "/NUM_SITES) ");
		}
		String endYear = "FY" + (15 + numColumns-3);
		StringBuilder systemFixedCostQuery = new StringBuilder(baseSystemFixedCostQuery.toString());
		systemFixedCostQuery.append(" FROM ").append(mainSustainmentFrame.getTableName())
					.append(" WHERE Central_Deployment='FALSE' GROUP BY SYSTEM;");
		updateSystemFixedCostValues(systemFixedCostQuery.toString(), endYear);
		
		LOGGER.info("DONE ::: Finsihed with fixed sustainment cost for systems where we do not have site specific costs...");
	}

	/**
	 * 
	 * @param query
	 * @param headers
	 * @param endYear
	 */
	private void updateSystemCostValues(String query, String endYear, double percentRealized) {
		ResultSet systemData = mainSustainmentFrame.execQuery(query);
		try {
			// then iterate add up all the data
			while(systemData.next()) {
				// get the system
				String system = systemData.getString(1);

				// iterate through and get the row data
				double[] newValues = new double[numColumns];
				for(int i = 1; i <= numColumns; i++) {
					newValues[i-1] = systemData.getDouble(i+1) * percentRealized;
				}
				
				// this will handle the specific system and values and perform the consolidation
				// if the system is already stored in the frame
				MhsGenesisDeploymentSavingsProcessor.updateCostValues(this.systemDeploymentSavings, system, newValues, endYear);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param query
	 * @param endYear
	 */
	private void updateSiteSpecificSystemCostValues(String query, String endYear) {
		ResultSet systemData = systemSiteSustainmentFrame.execQuery(query);
		try {
			// then iterate add up all the data
			while(systemData.next()) {
				// get the system
				String system = systemData.getString(1);
				// iterate through and get the row data
				double[] newValues = new double[numColumns];
				for(int i = 1; i <= numColumns; i++) {
					newValues[i-1] = systemData.getDouble(i+1);
				}
				
				// this will handle the specific system and values and perform the consolidation
				// if the system is already stored in the frame
				// since CHCS is the annoying thing we need to update
				// perform the check here
				String modEndYear = endYear;
				if(system.equals("CHCS")) {
					modEndYear = "FY" + ( Integer.parseInt(endYear.substring(2)) + 1 );
				}
				MhsGenesisDeploymentSavingsProcessor.updateCostValues(systemDeploymentSavings, system, newValues, modEndYear);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param query
	 * @param headers
	 * @param endYear
	 */
	private void updateSystemFixedCostValues(String query, String endYear) {
		ResultSet systemData = mainSustainmentFrame.execQuery(query);
		try {
			// then iterate add up all the data
			while(systemData.next()) {
				// get the system
				String system = systemData.getString(1);

				// iterate through and get the row data
				double[] newValues = new double[numColumns];
				for(int i = 1; i <= numColumns; i++) {
					newValues[i-1] = systemData.getDouble(i+1) * (1.0 - percentRealized);
				}
				
				String modEndYear = endYear;
				if(system.equals("CHCS")) {
					modEndYear = "FY" + ( Integer.parseInt(endYear.substring(2)) + 1 );
				}
				MhsGenesisDeploymentSavingsProcessor.updateCostValues(systemDeploymentSavings, system, newValues, modEndYear);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}



	
	
	
	
	
	


	/////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	//////////////////////////////////// MAIN METHOD ////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////
	/////////////////////////////////////////////////////////////////////////////////////////////

	/**
	 * Testing method
	 * @param args
	 */
	public static void main(String[] args) {
		TestUtilityMethods.loadDIHelper();

		String engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Core_Data.smss";
		IEngine coreEngine = new BigDataEngine();
		coreEngine.setEngineName("TAP_Core_Data");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("TAP_Core_Data");
		DIHelper.getInstance().setLocalProperty("TAP_Core_Data", coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Site_Data.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineName("TAP_Site_Data");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("TAP_Site_Data");
		DIHelper.getInstance().setLocalProperty("TAP_Site_Data", coreEngine);

		engineProp = "C:\\workspace\\Semoss_Dev\\db\\TAP_Portfolio.smss";
		coreEngine = new BigDataEngine();
		coreEngine.setEngineName("TAP_Portfolio");
		coreEngine.openDB(engineProp);
		coreEngine.setEngineName("TAP_Portfolio");
		DIHelper.getInstance().setLocalProperty("TAP_Portfolio", coreEngine);

		MhsGenesisDeploymentSavingsProcessor processor = new MhsGenesisDeploymentSavingsProcessor();
		MhsGenesisSystemDeploymentSavingsPlaySheet ps = new MhsGenesisSystemDeploymentSavingsPlaySheet(processor);
		ps.processDataMakerComponent(null);
	}


}