package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.util.DIHelper;

public class ResidualSystemTheatGarrOptFillData extends ResidualSystemOptFillData{
	protected static final Logger logger = LogManager.getLogger(ResidualSystemTheatGarrOptFillData.class.getName());
	
	public int[] systemTheater;
	public int[] systemGarrison;
	
	public int[][] dataRegionSORSystemTheaterExists;
	public int[][] dataRegionSORSystemGarrisonExists;
	public int[][] bluRegionProviderTheaterExists;
	public int[][] bluRegionProviderGarrisonExists;
	
	public boolean fillDataStores(boolean dataRequired,boolean includeTheater,boolean includeGarrison)
	{
		instantiate();
		runQueries();
		fillSystemTheaterGarrison(includeTheater,includeGarrison);
	
		dataRegionSORSystemCount = calculateIfProviderExistsWithRegion(systemDataMatrix,false);
		bluRegionProviderCount = calculateIfProviderExistsWithRegion(systemBLUMatrix,false);
		boolean reducedFunctionality = false;
		if(dataRequired && dataOrBLUWithNoProviderExists)
		{
			reducedFunctionality = true;
			dataRequired = false;
		}
		
		calculateIfProviderExistsWithRegion(systemDataMatrix,true,dataRequired);
		calculateIfProviderExistsWithRegion(systemBLUMatrix,false,dataRequired);
		
		if(playSheet!=null)
			printToConsole();
		else
			printAll();

		return reducedFunctionality;
	}
	
	private void fillSystemTheaterGarrison(boolean includeTheater,boolean includeGarrison)
	{
		String query = "SELECT DISTINCT ?System ?GT WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem>}{?System <http://semoss.org/ontologies/Relation/Contains/GarrisonTheater> ?GT}} BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);

		if(includeTheater) {
			systemTheater = new int[sysList.size()];
			for(int i=0;i<sysList.size();i++)
				systemTheater[i] = 0;
		}
		if(includeGarrison) {
			systemGarrison = new int[sysList.size()];
			for(int i=0;i<sysList.size();i++)
				systemGarrison[i] = 0;
		}
	
		SesameJenaSelectWrapper	wrapper = new SesameJenaSelectWrapper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(systemEngine);
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.setEngineType(IEngine.ENGINE_TYPE.SESAME);
		wrapper.executeQuery();	
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		while(wrapper.hasNext())
		{
			SesameJenaSelectStatement sjss = wrapper.next();
			String sysName = (String)getVariable(names[0], sjss);
			int rowIndex = sysList.indexOf(sysName);
			if(rowIndex>-1) {
				String theatGarr = (String)getVariable(names[1], sjss);
				theatGarr = theatGarr.toLowerCase();
				//if(systemTheater!=null && (theatGarr.contains("both")||theatGarr.contains("theater")))
				if(includeTheater && !theatGarr.contains("garrison"))
					systemTheater[rowIndex] = 1;

				//if(systemGarrison!=null && (theatGarr.contains("both")||theatGarr.contains("garrison")))
				if(includeGarrison && !theatGarr.contains("theater"))
					systemGarrison[rowIndex] = 1;
			}
		}
	}
	protected void calculateIfProviderExistsWithRegion(int[][] sysMatrix,boolean isData, boolean elementRequired)
	{
		int[][] theaterProviderExists = new int[sysMatrix[0].length][1];
		int[][] garrisonProviderExists = new int[sysMatrix[0].length][1];
		int regions = 1;
		if(includeRegionalization) {
			regions = regionList.size();
			theaterProviderExists = new int[sysMatrix[0].length][regionList.size()];
			garrisonProviderExists = new int[sysMatrix[0].length][regionList.size()];
		}
		//for every system in the system matrix
		for(int col=0;col<sysMatrix[0].length;col++)
		{
			for(int regionInd=0;regionInd<regions;regionInd++)
			{
				int numTheaterProviders = 0;
				int numGarrisonProviders = 0;
				if(elementRequired) {
					numTheaterProviders =1;
					numGarrisonProviders =1;
				}
				else {
					for(int row=0;row<sysMatrix.length;row++) {
						//check to see if that system is in the region we're currently looking at
						if(systemTheater!=null&&systemRegionMatrix[row][regionInd]>=1.0&&systemTheater[row]>=1)
							numTheaterProviders+=sysMatrix[row][col];
						if(systemGarrison!=null&&systemRegionMatrix[row][regionInd]>=1.0&&systemGarrison[row]>=1)
							numGarrisonProviders+=sysMatrix[row][col];
					}
				}
				if(numTheaterProviders==0&&numGarrisonProviders==0)
					dataOrBLUWithNoProviderExists = true;
				if(systemTheater!=null)
					theaterProviderExists[col][regionInd] =numTheaterProviders;
				if(systemGarrison!=null)
					garrisonProviderExists[col][regionInd] =numGarrisonProviders;
			}
		}
		if(isData) {
			if(systemTheater!=null)
				dataRegionSORSystemTheaterExists = theaterProviderExists;
			if(systemGarrison!=null)
				dataRegionSORSystemGarrisonExists = garrisonProviderExists;
		} else {
			if(systemTheater!=null)
				bluRegionProviderTheaterExists = theaterProviderExists;
			if(systemGarrison!=null)
				bluRegionProviderGarrisonExists = garrisonProviderExists;
		}
	}
	private void printAll()
	{
		System.out.println("System Provides Data:");
		printMatrix(systemDataMatrix,sysList,dataList);

		System.out.println("System Cost to Provide Data:");
		printMatrix(systemCostOfDataMatrix,sysList,dataList);
		
		System.out.println("System Provides BLU:");
		printMatrix(systemBLUMatrix,sysList,bluList);
		
		System.out.println("System Cost to Maintain:");
		printVector(systemCostOfMaintenance,sysList);
		System.out.println("System Cost to Maintain DB:");
		printVector(systemCostOfDB,sysList);
		System.out.println("System Site List:");
		printVector(systemNumOfSites,sysList);
		
		System.out.println("New site list must provide data in theater:");
		printMatrix(dataRegionSORSystemTheaterExists,dataList,regionList);
		System.out.println("New site list must provide data in garrison:");
		printMatrix(dataRegionSORSystemGarrisonExists,dataList,regionList);
		
		System.out.println("New site list must provide blu in theater:");
		printMatrix(bluRegionProviderTheaterExists,bluList,regionList);		
		System.out.println("New site list must provide blu in garrison:");
		printMatrix(bluRegionProviderGarrisonExists,bluList,regionList);
	}
	protected void printToConsole()
	{
		super.printToConsole();
		if(systemTheater!=null) {
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nTheater Systems: ");
			printNonZerosToConsoleList(deepCopy(sysList),systemTheater);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Theater Systems that are SOR of Data Object...");
			printNumberToConsoleList(deepCopy(dataList),dataRegionSORSystemTheaterExists);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Theater Systems that provide BLU by region...");
			printNumberToConsoleList(deepCopy(bluList),bluRegionProviderTheaterExists);
		}
		
		if(systemGarrison!=null) {
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nGarrison Systems: ");
			printNonZerosToConsoleList(deepCopy(sysList),systemGarrison);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Garrison Systems that are SOR of Data Object...");
			printNumberToConsoleList(deepCopy(dataList),dataRegionSORSystemGarrisonExists);
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Garrison Systems that provide BLU by region...");
			printNumberToConsoleList(deepCopy(bluList),bluRegionProviderGarrisonExists);
		}
	}
	private void printNonZerosToConsoleList(ArrayList<String> sysList,int[] listToCheck)
	{
		for(int i=0;i<listToCheck.length;i++)
			if(listToCheck[i]>0)
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+sysList.get(i)+", ");
	}
}
