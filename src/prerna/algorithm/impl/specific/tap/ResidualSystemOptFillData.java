package prerna.algorithm.impl.specific.tap;

import java.util.ArrayList;

import org.apache.log4j.Logger;

import prerna.rdf.engine.api.IEngine;
import prerna.rdf.engine.impl.SesameJenaSelectStatement;
import prerna.rdf.engine.impl.SesameJenaSelectWrapper;
import prerna.ui.components.specific.tap.DHMSMHelper;
import prerna.ui.components.specific.tap.SysOptPlaySheet;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class ResidualSystemOptFillData{
	
	SysOptPlaySheet playSheet=null;
	
	protected Logger logger = Logger.getLogger(getClass());
	ArrayList<String> sysList;
	ArrayList<String> dataList;
	ArrayList<String> bluList;
	
	//a_ip, b_iq, c_ip
	public int[][] systemDataMatrix;
	public int[][] systemBLUMatrix;
	public double[][] systemCostOfDataMatrix;
	
	//cM_i, cDM_i, s_i
	public double[] systemCostOfMaintenance;
	public double[] systemCostOfDB;
	public double[] systemNumOfSites;
	
	//Ap, Bq
	public int[] dataSORSystemExists;
	public int[] bluProviderExists;
	
	String systemEngine = "";
	String costEngine = "TAP_Cost_Data";
	String siteEngine = "TAP_Site_Data";
	
	String uri = "http://health.mil/ontologies/Concept/";
	String sysListBindings;
	double maxYears;
	
	public void setSysList(ArrayList<String> sysList)
	{
		this.sysList = sysList;
	}
	
	public void setDataList(ArrayList<String> dataList)
	{
		this.dataList = dataList;
	}
	
	public void setBLUList(ArrayList<String> bluList)
	{
		this.bluList = bluList;
	}
	
	public void setPlaySheet(SysOptPlaySheet playSheet)
	{
		this.playSheet = playSheet;
		this.systemEngine = playSheet.engine.getEngineName();
	}
	public void setMaxYears(double maxYears)
	{
		this.maxYears = maxYears;
	}
	
	/**
	 * Runs a query on a specific engine to make a list of systems to report on
	 * @param engineName 	String containing the name of the database engine to be queried
	 * @param query 		String containing the SPARQL query to run
	 */
	public ArrayList<String> runListQuery(String engineName, String query) {
		ArrayList<String> list = new ArrayList<String>();
		if(query!="")
		{
		try {
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
	
			SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
			wrapper.setQuery(query);
			wrapper.setEngine(engine);
			wrapper.executeQuery();
	
			String[] names = wrapper.getVariables();
			while (wrapper.hasNext()) {
				SesameJenaSelectStatement sjss = wrapper.next();
				list.add((String) sjss.getVar(names[0]));
				}
		} catch (Exception e) {
			Utility.showError("Cannot find engine: "+engineName);
		}
		}
		return list;
	}
	
	public ArrayList<String> deepCopy(ArrayList<String> list)
	{
		ArrayList<String> retList = new ArrayList<String>();
		for(String element : list)
		{
			retList.add(element);
		}
		return retList;
	}
	
	public void fillDataStores(boolean dataRequired)
	{
		systemDataMatrix = createEmptyMatrix(systemDataMatrix,sysList.size(),dataList.size());
		systemBLUMatrix = createEmptyMatrix(systemBLUMatrix,sysList.size(),bluList.size());
		systemCostOfDataMatrix = createEmptyMatrix(systemCostOfDataMatrix,sysList.size(),dataList.size());

		systemCostOfMaintenance = createEmptyVector(systemCostOfMaintenance, sysList.size());
		systemCostOfDB = createEmptyVector(systemCostOfDB, sysList.size());
		systemNumOfSites = createEmptyVector(systemNumOfSites, sysList.size());

		fillSystemData();
		fillSystemBLU();
		fillSystemCostOfData();
		
		fillSystemCost();
		fillSystemNumOfSites();
		
		dataSORSystemExists = calculateIfProviderExists(systemDataMatrix,false);
		bluProviderExists = calculateIfProviderExists(systemBLUMatrix,false);
		
		if(playSheet!=null)
			printToConsole();
		else
			printAll();
		dataSORSystemExists = calculateIfProviderExists(systemDataMatrix,dataRequired);
		bluProviderExists = calculateIfProviderExists(systemBLUMatrix,dataRequired);
	}
	
	public void printToConsole()
	{
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nSystems to be Considered...");
		printToConsoleList(sysList);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nData Objects Considered...");
		printToConsoleList(dataList);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Systems that are SOR of Data Object...");
		printNumberToConsoleList(deepCopy(dataList),dataSORSystemExists);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\nBLUs Considered...");
		printToConsoleList(bluList);
		playSheet.consoleArea.setText(playSheet.consoleArea.getText()+"\n*Number of Systems that provide BLU ...");
		printNumberToConsoleList(deepCopy(bluList),bluProviderExists);
	}
	
	public void printToConsoleList(ArrayList<String> list)
	{
		for(String entry : list)
		{
			playSheet.consoleArea.setText(playSheet.consoleArea.getText()+entry+", ");
		}
	}
	public void printNumberToConsoleList(ArrayList<String> list,int[] numbers)
	{
		ArrayList<Integer> numList = new ArrayList<Integer>();
		for(int i=0;i<numbers.length;i++)
			numList.add(numbers[i]);
		int numSystemsIndex = 0;
		while(!numList.isEmpty())
		{
			int listLoc = numList.indexOf(numSystemsIndex);
			if(listLoc>-1)
			{
				playSheet.consoleArea.setText(playSheet.consoleArea.getText()+list.get(listLoc)+": "+numSystemsIndex+", ");
				list.remove(listLoc);
				numList.remove(listLoc);
			}
			else
				numSystemsIndex++;
		}
	}
	
	public void printAll()
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
		
		System.out.println("New site list must provide data:");
		printVector(dataSORSystemExists,dataList);

		System.out.println("New site list must provide blu:");
		printVector(bluProviderExists,bluList);
	}
	
	private void printMatrix(int[][] matrix, ArrayList<String> rowList,ArrayList<String> colList)
	{
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			for(int j=0;j<matrix[0].length;j++)
			{
				if(matrix[i][j]>0)
					System.out.println(rowEntry + "..."+colList.get(j));
			}
		}
	}
	
	private void printMatrix(double[][] matrix, ArrayList<String> rowList,ArrayList<String> colList)
	{
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			for(int j=0;j<matrix[0].length;j++)
			{
				if(matrix[i][j]>0.0)
					System.out.println(rowEntry + "..."+colList.get(j)+"..."+matrix[i][j]);
			}
		}
	}
	
	private void printVector(double[] matrix, ArrayList<String> rowList)
	{
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			if(matrix[i]>0.0)
				System.out.println(rowEntry + "..."+matrix[i]);
		}
	}
	
	private void printVector(int[] matrix, ArrayList<String> rowList)
	{
		for(int i=0;i<matrix.length;i++)
		{
			String rowEntry = rowList.get(i);
			if(matrix[i]>0.0)
				System.out.println("Provider exists for..."+rowEntry);
		}
	}
	
	private int[][] createEmptyMatrix(int[][] matrix, int row,int col)
	{
		matrix = new int[row][col];
		for(int x=0;x<row;x++)
			for(int y=0;y<col;y++)
				matrix[x][y] = 0;
		return matrix;
	}
	private double[][] createEmptyMatrix(double[][] matrix, int row,int col)
	{
		matrix = new double[row][col];
		for(int x=0;x<row;x++)
			for(int y=0;y<col;y++)
				matrix[x][y] = 0;
		return matrix;
	}

	private double[] createEmptyVector(double[] matrix, int row)
	{
		matrix = new double[row];
		for(int x=0;x<row;x++)
			matrix[x] = 0;
		return matrix;
	}
	
	public void fillSystemData()
	{
		DHMSMHelper dhelp = new DHMSMHelper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(systemEngine);
		dhelp.setUseDHMSMOnly(false);
		dhelp.runData(engine);
		
		for(int sysInd = 0;sysInd < sysList.size();sysInd++)
		{
			String sys = sysList.get(sysInd);
			ArrayList<String> dataObjects = dhelp.getAllDataFromSys(sys, "C");
			systemDataMatrix=fillSysRow(systemDataMatrix, sysInd, dataList, dataObjects);
		}
	}
	
	public void fillSystemBLU()
	{
		String query = "SELECT DISTINCT ?System ?blu WHERE{{?blu <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/BusinessLogicUnit>}{?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/ActiveSystem> } {?provideBLU <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/Provide>} {?System ?provideBLU ?blu}} BINDINGS ?System @SYSTEM-BINDINGS@";
		
		sysListBindings = makeBindingString("System",sysList);		
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		
		systemBLUMatrix = fillMatrixFromQuery(systemEngine,query,systemBLUMatrix,sysList,bluList,true);
		
	}
	
	public void fillSystemCostOfData()
	{
		String query = "SELECT DISTINCT ?sys ?data (SUM(?loe)*150 AS ?cost) WHERE { BIND( <http://health.mil/ontologies/Concept/GLTag/Provider> AS ?gltag) {?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>} {?phase <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SDLCPhase>} {?subclass <http://www.w3.org/2000/01/rdf-schema#subClassOf> <http://semoss.org/ontologies/Concept/TransitionGLItem> ;} {?GLitem <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?subclass}{?GLitem <http://semoss.org/ontologies/Relation/TaggedBy> ?gltag;} {?data <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DataObject>}{?ser <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/Service> ;}{?sys <http://semoss.org/ontologies/Relation/Influences> ?GLitem} {?GLitem <http://semoss.org/ontologies/Relation/Contains/LOEcalc> ?loe;}  {?phase <http://semoss.org/ontologies/Relation/Contains/StartDate> ?start}  {?GLitem <http://semoss.org/ontologies/Relation/BelongsTo> ?phase} {?GLitem <http://semoss.org/ontologies/Relation/Output> ?ser }{?data <http://semoss.org/ontologies/Relation/Input> ?GLitem}} GROUP BY ?sys ?data BINDINGS ?sys @SYSTEM-BINDINGS@";
		
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		
		systemCostOfDataMatrix = fillMatrixFromQuery(costEngine,query,systemCostOfDataMatrix,sysList,dataList,false);
	}
	
	public void fillSystemCost()
	{
		String query = "SELECT DISTINCT ?sys (COALESCE(?cost,0) AS ?Cost) WHERE {{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?sys <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}}BINDINGS ?sys @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCostOfMaintenance = fillVectorFromQuery(systemEngine,query,systemCostOfMaintenance,sysList,false);
		
		query = "SELECT DISTINCT ?sys (COALESCE(?cost/10,0) AS ?Cost) WHERE {{?sys <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>}{?sys <http://semoss.org/ontologies/Relation/Contains/SustainmentBudget> ?cost}}BINDINGS ?sys @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemCostOfDB = fillVectorFromQuery(systemEngine,query,systemCostOfDB,sysList,false);
	}
	
	public void fillSystemNumOfSites()
	{
		String query = "SELECT DISTINCT ?System (COUNT(DISTINCT(?DCSite)) as ?Num_Of_Deployment_Sites) WHERE { {?System <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/System>;} {?SystemDCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/SystemDCSite>;} {?DeployedAt <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DeployedAt1 <http://www.w3.org/2000/01/rdf-schema#subPropertyOf> <http://semoss.org/ontologies/Relation/DeployedAt>;} {?DCSite <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://semoss.org/ontologies/Concept/DCSite>;} {?SystemDCSite ?DeployedAt ?DCSite;} {?System ?DeployedAt1 ?SystemDCSite;} } GROUP BY ?System BINDINGS ?System @SYSTEM-BINDINGS@";
		query = query.replace("@SYSTEM-BINDINGS@",sysListBindings);
		systemNumOfSites = fillVectorFromQuery(siteEngine,query,systemNumOfSites,sysList,false);
	}
	
	public int[][] fillSysRow(int[][] matrixToFill,int rowInd, ArrayList<String> colList, ArrayList<String> colToPopulate)
	{
		for(int ind = 0;ind<colToPopulate.size();ind++)
		{
			String colName = colToPopulate.get(ind);
			int matrixColInd = colList.indexOf(colName);
			if(matrixColInd>-1)
				matrixToFill[rowInd][matrixColInd] = 1;
		}
		return matrixToFill;
	}
	
	public SesameJenaSelectWrapper runQuery(String engineName, String query)
	{
		SesameJenaSelectWrapper wrapper = new SesameJenaSelectWrapper();
		IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		wrapper.setQuery(query);
		wrapper.setEngine(engine);
		wrapper.setEngineType(IEngine.ENGINE_TYPE.SESAME);
		try{
			wrapper.executeQuery();	
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		return wrapper;
	}
	
	public int[][] fillMatrixFromQuery(String engineName, String query,int[][] matrix,ArrayList<String> rowNames,ArrayList<String> colNames,boolean valIsOne) {
		SesameJenaSelectWrapper	wrapper = runQuery(engineName,query);

		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				Object rowName = getVariable(names[0], sjss);
				Object colName = getVariable(names[1], sjss);
				
				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex>-1)
				{
					int colIndex = colNames.indexOf(colName);
					if(colIndex>-1)
					{
						if(valIsOne)
							matrix[rowIndex][colIndex] = 1;
						else
						{
							matrix[rowIndex][colIndex] = (Integer)getVariable(names[2], sjss);
						}
					}
				}

			}
		} catch (Exception e) {
			logger.fatal(e);
		}
		return matrix;
	}
	
	public double[][] fillMatrixFromQuery(String engineName, String query,double[][] matrix,ArrayList<String> rowNames,ArrayList<String> colNames,boolean valIsOne) {
		SesameJenaSelectWrapper	wrapper = runQuery(engineName,query);

		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				Object rowName = getVariable(names[0], sjss);
				Object colName = getVariable(names[1], sjss);
				
				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex>-1)
				{
					int colIndex = colNames.indexOf(colName);
					if(colIndex>-1)
					{
						if(valIsOne)
							matrix[rowIndex][colIndex] = 1.0;
						else
						{
							matrix[rowIndex][colIndex] = (Double)getVariable(names[2], sjss);
						}
					}
				}

			}
		} catch (Exception e) {
			logger.fatal(e);
		}
		return matrix;
	}
	
	public int[] fillVectorFromQuery(String engineName, String query,int[] matrix,ArrayList<String> rowNames, boolean valIsOne) {
		SesameJenaSelectWrapper	wrapper = runQuery(engineName,query);

		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				Object rowName = getVariable(names[0], sjss);
				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex>-1)
				{
					if(valIsOne)
						matrix[rowIndex]= 1;
					else
						matrix[rowIndex]= (Integer)getVariable(names[1], sjss);
				}
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
		return matrix;
	}
	
	public double[] fillVectorFromQuery(String engineName, String query,double[] matrix,ArrayList<String> rowNames, boolean valIsOne) {
		SesameJenaSelectWrapper	wrapper = runQuery(engineName,query);

		// get the bindings from it
		String[] names = wrapper.getVariables();
		// now get the bindings and generate the data
		try {
			while(wrapper.hasNext())
			{
				SesameJenaSelectStatement sjss = wrapper.next();
				Object rowName = getVariable(names[0], sjss);
				int rowIndex = rowNames.indexOf(rowName);
				if(rowIndex>-1)
				{
					if(valIsOne)
						matrix[rowIndex]= 1.0;
					else
						matrix[rowIndex]= (Double)getVariable(names[1], sjss);
				}
			}
		} catch (Exception e) {
			logger.fatal(e);
		}
		return matrix;
	}
	
	public Object getVariable(String varName, SesameJenaSelectStatement sjss){
		return sjss.getVar(varName);
	}
	
	public String makeBindingString(String type,ArrayList<String> vals)
	{
		String bindings = "{";
		for(String val : vals)
		{
			bindings+= "(<"+uri+type+"/"+val+">)";
		}
		bindings+="}";
		return bindings;
	}
	
	public int[] calculateIfProviderExists(int[][] sysMatrix,boolean elementRequired)
	{
		int[] retVector = new int[sysMatrix[0].length];
		for(int col=0;col<sysMatrix[0].length;col++)
		{
			int numProviders = 0;
			if(elementRequired)
				numProviders =1;
			else
			{
				for(int row=0;row<sysMatrix.length;row++)
				{
					numProviders+=sysMatrix[row][col];
				}
			}
			retVector[col] =numProviders;
		}
		return retVector;
	}
}
