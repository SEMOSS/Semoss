package prerna.sablecc;

import java.io.File;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Map;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngineException;
import org.rosuda.REngine.RFactor;
import org.rosuda.REngine.RList;
import org.rosuda.REngine.Rserve.RConnection;
import org.rosuda.REngine.Rserve.RserveException;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.engine.impl.r.RSingleton;
import prerna.util.Utility;

public class BaseJavaReactor extends AbstractRJavaReactor{

	public BaseJavaReactor() {
		super();
	}
	
	public BaseJavaReactor(ITableDataFrame frame) {
		super(frame);
	}
	
	/**
	 * Starts the connection to R
	 */
	@Override
	protected Object startR() {
		// we store the connection in the PKQL Runner
		// retrieve it if it is already defined within the insight
		RConnection retCon = (RConnection) retrieveVariable(R_CONN);
		String port = (String) retrieveVariable(R_PORT);
		LOGGER.info("Connection right now is set to.. " + retCon);
		if(retCon == null) {
			try {
				RConnection masterCon = RSingleton.getConnection();
				port = Utility.findOpenPort();
				
				LOGGER.info("Starting it on port.. " + port);
				// need to find a way to get a common name
				masterCon.eval("library(Rserve); Rserve(port = " + port + ")");
				retCon = new RConnection("127.0.0.1", Integer.parseInt(port));
				// load all the libraries
				retCon.eval("library(splitstackshape);");
				// data table
				retCon.eval("library(data.table);");
				// reshape2
				retCon.eval("library(reshape2);");
				// rjdbc
				retCon.eval("library(RJDBC);");
				// stringr
				retCon.eval("library(stringr)");
			} catch (Exception e) {
				System.out.println("ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
						+ "1)Rserve\n"
						+ "2)splitstackshape\n"
						+ "3)data.table\n"
						+ "4)reshape2\n"
						+ "5)RJDBC*\n"
						+ "6)stringr\n\n"
						+ "*Please note RJDBC might require JAVA_HOME environment path to be defined on your system.");
				e.printStackTrace();
			}
		}
		storeVariable(AbstractRJavaReactor.R_CONN, retCon);
		storeVariable(AbstractRJavaReactor.R_PORT, port);
		return retCon;
	}
	
	/**
	 * Get the current working directory of the R session
	 */
	@Override
	protected String getWd() {
		RConnection retCon = (RConnection) startR();
		try {
			return retCon.eval("getwd()").asString();
		} catch (RserveException | REXPMismatchException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Reconnect the main R server port
	 * @param port
	 */
	public void reconnectR(int port) {
		RSingleton.getConnection(port);
	}
	
	/**
	 * Execute a rScript
	 */
	public Object eval(String script)
	{
		RConnection rcon = (RConnection)startR();
		try {
			return rcon.eval(script);
		} catch (RserveException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	// remove the node on R
	// get the number of clustered components
	// perform a layout
	// color a graph based on a formula
	public void key()
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");
		String names = "";
		RConnection con = (RConnection)startR();
		try {
			// get the articulation points
			int [] vertices = con.eval("articulation.points(" + graphName + ")").asIntegers();
			// now for each vertex get the name
			Hashtable <String, String> dataHash = new Hashtable<String, String>();
			for(int vertIndex = 0;vertIndex < vertices.length;  vertIndex++)
			{
				String output = con.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\", " + vertices[vertIndex] + ")").asString();
				String [] typeData = output.split(":");
				String typeOutput = "";
				if(dataHash.containsKey(typeData[0]))
					typeOutput = dataHash.get(typeData[0]);
				typeOutput = typeOutput + "  " + typeData[1];
				dataHash.put(typeData[0], typeOutput);
			}
			
			Enumeration <String> keys = dataHash.keys();
			while(keys.hasMoreElements())
			{
				String thisKey = keys.nextElement();
				names = names + thisKey + " : " + dataHash.get(thisKey) + "\n";
			}
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println(" Key Nodes \n " + names);
	}
	
	public void colorClusters(String clusterName)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		RConnection rcon = (RConnection)startR();
		// the color is saved as color
		try
		{
			int [] memberships = rcon.eval(clusterName + "$membership").asIntegers();
			String [] IDs = rcon.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")").asStrings();
			
			for(int memIndex = 0;memIndex < memberships.length;memIndex++)
			{
				String thisID = IDs[memIndex];

				java.lang.System.out.println("ID...  " + thisID);
				Vertex retVertex = null;
				
				GraphTraversal<Vertex, Vertex>  gt = ((TinkerFrame)dataframe).g.traversal().V().has(TinkerFrame.TINKER_ID, thisID);
				if(gt.hasNext()) {
					retVertex = gt.next();
				}
				if(retVertex != null)
				{
					retVertex.property("CLUSTER", memberships[memIndex]);
					java.lang.System.out.println("Set the cluster to " + memberships[memIndex]);
				}
			}
		}catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void getResultAsString(Object output, StringBuilder builder)
	{
		// Generic vector..
		if(output instanceof REXPGenericVector) 
		{			
			RList list = ((REXPGenericVector)output).asList();
			
			String[] attributeNames = getAttributeArr(((REXPGenericVector)output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if(attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if(!matchesRows) {
					if(attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if(attributeNames.length > 1){
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for(int listIndex = 0; listIndex < size; listIndex++) {
				if(matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}
		
		// List..
		else if(output instanceof REXPList) {
			RList list = ((REXPList)output).asList();
			
			String[] attributeNames = getAttributeArr(((REXPList)output)._attr());
			boolean matchesRows = false;
			// output list attribute names if present
			if(attributeNames != null) {
				// Due to the way R sends back data
				// When there is a list, it may contain a name label
				matchesRows = list.size() == attributeNames.length;
				if(!matchesRows) {
					if(attributeNames.length == 1) {
						builder.append("\n" + attributeNames[0] + "\n");
					} else if(attributeNames.length > 1){
						builder.append("\n" + Arrays.toString(attributeNames) + "\n");
					}
				}
			}
			int size = list.size();
			for(int listIndex = 0; listIndex < size; listIndex++) {
				if(matchesRows) {
					builder.append("\n" + attributeNames[listIndex] + " : ");
				}
				getResultAsString(list.get(listIndex), builder);
			}
		}
		
		// Integers..
		else if(output instanceof REXPInteger)
		{
			int [] ints =  ((REXPInteger)output).asIntegers();
			if(ints.length > 1)
			{
				for(int intIndex = 0;intIndex < ints.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(ints[intIndex]);
					} else {
						builder.append(" ").append(ints[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(ints[0]);
			}
		}
		
		// Doubles.. 
		else if(output instanceof REXPDouble)
		{
			double [] doubles =  ((REXPDouble)output).asDoubles();
			if(doubles.length > 1)
			{
				for(int intIndex = 0;intIndex < doubles.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(doubles[intIndex]);
					} else {
						builder.append(" ").append(doubles[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(doubles[0]);
			}
		}
		
		// Strings..
		else if(output instanceof REXPString)
		{
			String [] strings =  ((REXPString)output).asStrings();
			if(strings.length > 1)
			{				
				for(int intIndex = 0;intIndex < strings.length; intIndex++) {
					if(intIndex == 0) {
						builder.append(strings[intIndex]);
					} else {
						builder.append(" ").append(strings[intIndex]);
					}
				}
			}
			else
			{					
				builder.append(strings[0]);
			}
		}
		
		builder.append("\n");
	}
	
	private String[] getAttributeArr(REXPList attrList) {
		if(attrList == null) {
			return null;
		}
		if(attrList.length() > 0) {	
			Object attr = attrList.asList().get(0);
			if(attr instanceof REXPString) {
				String[] strAttr = ((REXPString)attr).asStrings();
				return strAttr;
			}
		}
		return null;
	}
	
	
	public void runR(String script)
	{
		runR(script, true);
	}
	
	public void runR(String script, boolean result)
	{
		RConnection rcon = (RConnection)startR();
		String newScript = "paste(capture.output(print(" + script + ")),collapse='\n')";
		try {
			REXP output = rcon.parseAndEval(newScript);
			if(result) {
				System.out.println(output.asString());
			}
		} catch(REngineException | REXPMismatchException e) {
			try {
				Object output = rcon.eval(script);
				if(result)
				{
					java.lang.System.out.println("RCon data.. " + output);
					StringBuilder builder = new StringBuilder();
					getResultAsString(output, builder);
					System.out.println("Output : " + builder.toString());
				}
			} catch (REngineException e2) {
				e2.printStackTrace();
				String errorMessage = null;
				if(e2.getMessage() != null && !e2.getMessage().isEmpty()) {
					errorMessage = e2.getMessage();
				} else {
					errorMessage = "Unexpected error in execution of R routine ::: " + script;
				}
				throw new IllegalArgumentException(errorMessage);
			}
		}
	}

	// split a column based on a value
	public void performSplitColumn(String frameName, String columnName, String separator, boolean dropColumn, boolean frameReplace)
	{
		//  cSplit(dt, "PREFIX", "_")
		RConnection rcon = (RConnection) startR();
		// need to get the type of this
		try {
			String tempName = Utility.getRandomString(8);
			
			String frameReplaceScript = frameName + " <- " + tempName + ";";
			if(!frameReplace)
				frameReplaceScript = "";
			String columnReplaceScript = "TRUE";
			if(!dropColumn)
				columnReplaceScript = "FALSE";
			String script = tempName + " <- cSplit(" + frameName + ", \"" + columnName + "\", \"" + separator + "\", drop = " + columnReplaceScript+ ");" 
				//+ tempName +" <- " + tempName + "[,lapply(.SD, as.character)];"  // this ends up converting numeric to factors too
				//+ frameReplaceScript
				;
			rcon.eval(script);
			System.out.println("Script " + script);
			// get all the columns that are factors
			script = "sapply(" + tempName + ", is.factor);";
			String [] factors = rcon.eval(script).asStrings();			
			String [] colNames = getColNames(tempName);
			
			// now I need to compose a string based on it
			String conversionString = "";
			for(int factorIndex = 0;factorIndex < factors.length;factorIndex++)
			{
				if(factors[factorIndex].equalsIgnoreCase("TRUE")) // this is a factor
				{
					conversionString = conversionString + 
							tempName + "$" + colNames[factorIndex] + " <- "
							+ "as.character(" + tempName + "$" +colNames[factorIndex] + ");";
				}
			}
			rcon.eval(conversionString + frameReplaceScript);
			
			// perform variable cleanup
			// perform variable cleanup
			rcon.eval("rm(" + tempName + ");");
			rcon.eval("gc();");
			
			System.out.println("Script " + script);
			System.out.println("Complete ");
			// once this is done.. I need to find what the original type is and then apply a type to it
			// may be as string
			// or as numeric
			// else this is not giving me what I want :(
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
		
	public String[] getColNames(String frameName, boolean print) {
		RConnection rcon = (RConnection) startR();
		String [] colNames = null;
		try {
			String script = "names(" + frameName + ");";
			colNames = rcon.eval(script).asStrings();
			if(print)
			{
				System.out.println("Columns..");
				for(int colIndex = 0;colIndex < colNames.length; colIndex++) {
					System.out.println(colNames[colIndex] + "\n");
				}
			}
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return colNames;
	}
	
	public String getColType(String frameName, String colName, boolean print) {
		RConnection rcon = (RConnection) startR();
		String colType = null;
		try {
			String script = "sapply(" + frameName + "$" + colName + ", class);";
			colType = rcon.eval(script).asString();
			if(print) {
				System.out.println(colName + "has type " + colType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return colType;
	}

	public String[] getColTypes(String frameName, boolean print) {
		RConnection rcon = (RConnection) startR();
		String [] colTypes = null;
		try {
			String script = "matrix(sapply(" + frameName + ", class));";
			colTypes = rcon.eval(script).asStrings();
			if(print) {
				System.out.println("Columns..");
				for(int colIndex = 0; colIndex < colTypes.length; colIndex++) {
					System.out.println(colTypes[colIndex] + "\n");
				}
			}
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return colTypes;
	}

	// gives the different types of columns that are there and how many are there of that type
	// such as 5 integer columns
	// 3 string columns
	// 4 blank columns etc. 
	public void getColumnTypeCount(String frameName)
	{
		RConnection rcon = (RConnection) startR();
		Object [][] retOutput = null; // name and the number of items
		
		try {
			String script = "";
			
			String [] colNames = getColNames(frameName);
			// I am not sure if I need the colnames right now but...
			
			// get the column types
			String [] colTypes = getColTypes(frameName);
			
			// get the blank columns
			script = "matrix( " + frameName + "[, colSums( " + frameName + " != \"\") !=0])";
			String [] blankCols = rcon.eval(script).asStrings();
			
			Hashtable <String, Integer> colCount = new Hashtable <String, Integer>();
			
			for(int colIndex = 0;colIndex < colTypes.length;colIndex++)
			{
				String colType = colTypes[colIndex];
				if(blankCols[colIndex].equalsIgnoreCase("FALSE"))
					colType = "Empty";
				int count = 0;
				if(colCount.containsKey(colType))
					count = colCount.get(colType);
				
				count++;
				colCount.put(colType, count);
			}
			
			StringBuilder builder = new StringBuilder();
			builder.append(colCount + "");
			System.out.println("Output : " + builder.toString());
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void performJoinColumns(String frameName, String newColumnName,  String separator, String cols)
	{
		// reconstruct the column names
		//paste(df1$a_1, df1$a_2, sep="$$")
		try {
			RConnection rcon = (RConnection) startR();
			String [] columns = cols.split(";");
			String concatString = "paste(";
			for(int colIndex = 0;colIndex < columns.length;colIndex++)
			{
				concatString = concatString + frameName + "$" + columns[colIndex];
				if(colIndex + 1 < columns.length)
					concatString = concatString + ", ";
			}
			concatString = concatString + ", sep= \"" + separator + "\")";
			
			String script = frameName + "$" + newColumnName + " <- " + concatString;
			System.out.println(script);
			rcon.eval(script);
			System.out.println("Join Complete ");
		
		} catch (RserveException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Object[][] getColumnCount(String frameName, String column, boolean print)
	{
		// get all the column names first
		/*
		 * colnames(frame) <-- this can probably be skipped because we have the metadata
		 * class(frame$name)
		 * frame[, .N, by="column"] <-- gives me all the counts of various values
		 * matrix(sapply(DT, class), byrow=TRUE) <-- makes it into a full array so I can ick through the types
		 * strsplit(x,".", fixed=T) splits the string based on a delimiter.. fixed says is it based on regex or not 
		 * dt[PY == "hello", PY := "D"] replaces a column conditionally based on the value
		 * dt[, val4:=""] <-- add a new column called val 4
		 * matrix(dt[, colSums(dt != "") !=0]) <-- gives me if a column is blank or not.. 
		 * 
		 */
		// start the R first
		RConnection rcon = (RConnection) startR();
		Object [][] retOutput = null; // name and the number of items
		
		try {
			String tempName = Utility.getRandomString(6);
			String script = tempName + " <-  " + frameName + "[, .N, by=\"" + column +"\"];";
			System.out.println("Script is " + script);
			rcon.eval(script);
			
			script = tempName + "$" + column;
			String [] uniqueColumns = rcon.eval(script).asStrings();
			if(uniqueColumns == null) {
				RFactor factors = rcon.eval(script).asFactor();
				int numFactors = factors.size();
				uniqueColumns = new String[numFactors];
				for(int i = 0; i < numFactors; i++) {
					uniqueColumns[i] = factors.at(i);
				}
			} 
			// need to limit this eventually to may be 10-15 and no more
			script = "matrix(" + tempName + "$N);"; 
			int [] colCount = rcon.eval(script).asIntegers();
			retOutput = new Object[uniqueColumns.length][2];
			StringBuilder builder = null;
			if(print) {
				builder = new StringBuilder();
				builder.append(column + "\t Count \n");
			}
			for(int outputIndex = 0;outputIndex < uniqueColumns.length; outputIndex++) {
				retOutput[outputIndex][0] = uniqueColumns[outputIndex];
				retOutput[outputIndex][1] = colCount[outputIndex];
				if(print) {
					builder.append(retOutput[outputIndex][0] + "\t" + retOutput[outputIndex][1] + "\n");
				}
			}
			if(print) {
				System.out.println("Output : " + builder.toString());
			} else {
				// create the weird object the FE needs to paint a bar chart
				this.returnData = getBarChartInfo(column, "Frequency", retOutput);
				this.hasReturnData = true;
			}
			
			// perform variable cleanup
			rcon.eval("rm(" + tempName + ");");
			rcon.eval("gc();");
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return retOutput;
	}
	
	@Override
	protected int getNumRows(String frameName) {
		RConnection rcon = (RConnection) startR();
		int numRows = 0;
		try {
			numRows = rcon.eval("nrow(" + frameName + ")").asInteger();
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return numRows;
	}
	
	public Object[][] getDescriptiveStats(String frameName, String column, boolean print) {
		RConnection rcon = (RConnection) startR();
		Object [][] retOutput = new Object[8][2]; // name and the number of items
		try {
			String frameExpr = frameName + "$" + column;
			String script = "min(as.numeric(na.omit(" + frameExpr + ")))";
			double min = rcon.eval(script).asDouble();
			retOutput[0][0] = "Minimum";
			retOutput[0][1] = min;
			
			script = "quantile(as.numeric(na.omit(" + frameExpr + ")), prob = c(0.25, 0.75))";
			double[] quartiles = rcon.eval(script).asDoubles();
			retOutput[1][0] = "Q1";
			retOutput[1][1] = quartiles[0];
			retOutput[2][0] = "Q3";
			retOutput[2][1] = quartiles[1];
			
			script = "max(as.numeric(na.omit(" + frameExpr + ")))";
			double max = rcon.eval(script).asDouble();
			retOutput[3][0] = "Maximum";
			retOutput[3][1] = max;
			
			script = "mean(as.numeric(na.omit(" + frameExpr + ")))";
			double mean = rcon.eval(script).asDouble();
			retOutput[4][0] = "Mean";
			retOutput[4][1] = mean;
			
			script = "median(as.numeric(na.omit(" + frameExpr + ")))";
			double median = rcon.eval(script).asDouble();
			retOutput[5][0] = "Median";
			retOutput[5][1] = median;
			
			script = "sum(as.numeric(na.omit(" + frameExpr + ")))";
			double sum = rcon.eval(script).asDouble();
			retOutput[6][0] = "Sum";
			retOutput[6][1] = sum;
			
			script = "sd(as.numeric(na.omit(" + frameExpr + ")))";
			double sd = rcon.eval(script).asDouble();
			retOutput[7][0] = "Standard Deviation";
			retOutput[7][1] = sd;
			
			if(print) {
				StringBuilder builder = new StringBuilder();
				builder.append("Summary Stats\n");
				for(int outputIndex = 0;outputIndex < retOutput.length; outputIndex++) {
					builder.append(retOutput[outputIndex][0] + "\t" + retOutput[outputIndex][1] + "\n");
				}
				System.out.println("Output : " + builder.toString());
			} else {
				this.hasReturnData = true;
				this.returnData = retOutput;
			}
		} catch (RserveException e) {
			e.printStackTrace();
		} catch (REXPMismatchException e) {
			e.printStackTrace();
		}
		return retOutput;
	}
	
	public Object[][] getHistogram(String frameName, String column, int numBreaks, boolean print) {
		Object[][] data = null;
		
		RConnection rcon = (RConnection) startR();
		String script = null;
		if(numBreaks > 1) {
			script = "hist(" + frameName + "$" + column + ", breaks=" + numBreaks + ", plot=FALSE)";
		} else {
			script = "hist(" + frameName + "$" + column + ", plot=FALSE)";
		}
		System.out.println("Script is " + script);
		try {
			REXP histR = rcon.eval(script);
			Map<String, Object> histJ = (Map<String, Object>) histR.asNativeJavaObject();
			
			// so we know a bit about the structure
			// we can get the following values
			// 1: breaks
			// 2: counts
			// 3: density
			// 4: mids
			// 5: xname
			// 6: equidist
	
			// we only need the breaks and counts
			// format each range to the count value
			double[] breaks = (double[]) histJ.get("breaks");
			int[] counts = (int[]) histJ.get("counts");
			int numBins = counts.length;
			data = new Object[numBins][2];
	
			if(print) {
				System.out.println("Generating histogram for column = " + column);
			} else {
				// create the weird object the FE needs to paint a bar chart
				this.returnData = getBarChartInfo(column, "Frequency", data);
				this.hasReturnData = true;
			}
	
			for(int i = 0; i < numBins; i++) {
				data[i][0] = breaks[i] + " - " + breaks[i+1];
				data[i][1] = counts[i];
				if(print) {
					System.out.println(data[i][0] + "\t\t" + data[i][1]);
				}
			}
		} catch (RserveException | REXPMismatchException e) {
			e.printStackTrace();
		}
		
		return data;
	}
	
	public void unpivot(String frameName, String cols, boolean replace)
	{
		// makes the columns and converts them into rows
		// need someway to indicate to our metadata as well that this is gone
		//melt(dat, id.vars = "FactorB", measure.vars = c("Group1", "Group2"))
		
		String concatString = "";
		RConnection rcon = (RConnection) startR();
		String tempName = Utility.getRandomString(8);
		
		if(cols != null && cols.length() > 0)
		{
			String [] columnsToPivot = cols.split(";");
			concatString = ", measure.vars = c(";
			for(int colIndex = 0;colIndex < columnsToPivot.length;colIndex++)
			{
				concatString = concatString + "\"" + columnsToPivot[colIndex] + "\"";
				if(colIndex + 1 < columnsToPivot.length)
					concatString = concatString + ", ";
			}
			concatString = concatString + ")";
		}
		String replacer = "";
		if(replace)
			replacer = frameName + " <- " + tempName;
		String script = tempName + "<- melt(" + frameName + concatString + ");" 
//						+ tempName + " <- " + tempName + "[,lapply(.SD, as.character)];"
						+ replacer;
		System.out.println("executing script " + script);
		try {
			rcon.eval(script);
			if(replace && checkRTableModified(frameName)) {
				recreateMetadata(frameName);
			}
		} catch(Exception ex) {
			ex.printStackTrace();
		}
	}

	public void pivot(String frameName, boolean replace,String columnToPivot, String cols)
	{
		// makes the columns and converts them into rows
		
		//dcast(molten, formula = subject~ variable)
		// I need columns to keep and columns to pivot
		
		RConnection rcon = (RConnection) startR();
		String newFrame = Utility.getRandomString(8);
		
		String replaceString = "";
		if(replace)
			replaceString = frameName + " <- " + newFrame;
		
		String keepString = ""; 	
		if(cols != null && cols.length() > 0)
		{
			String [] columnsToKeep = cols.split(";");
			keepString = ", formula = ";
			for(int colIndex = 0;colIndex < columnsToKeep.length;colIndex++)
			{
				keepString = keepString + "\"" + columnsToKeep[colIndex] + "\"";
				if(colIndex + 1 < columnsToKeep.length)
					keepString = keepString + " + ";
			}
			keepString = keepString + " ~ " + columnToPivot;
		}
		
		String script = newFrame + " <- dcast(" + frameName + keepString + ");"
						+ replaceString ;
		System.out.println("executing script " + script);
		try {
			rcon.eval(script);
			if(replace && checkRTableModified(frameName)) {
				recreateMetadata(frameName);
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public void synchronizeXY(String rVariable)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		RConnection rcon = (RConnection)startR();
		try
		{
			double [][] memberships = rcon.eval("xy_layout").asDoubleMatrix();
			String [] axis = null;
			if(memberships[0].length == 2) {
				axis = new String[]{"X", "Y"};
			} else if(memberships[0].length == 3) {
				axis = new String[]{"X", "Y", "Z"};
			}
			
			String [] IDs = rcon.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")").asStrings();
			
			for(int memIndex = 0; memIndex < memberships.length; memIndex++)
			{
				String thisID = IDs[memIndex];
	
				java.lang.System.out.println("ID...  " + thisID);
				Vertex retVertex = null;
				
				GraphTraversal<Vertex, Vertex>  gt = ((TinkerFrame)dataframe).g.traversal().V().has(TinkerFrame.TINKER_ID, thisID);
				if(gt.hasNext()) {
					retVertex = gt.next();
				}
				if(retVertex != null)
				{
					for(int i = 0; i < axis.length; i++) {
						retVertex.property(axis[i], memberships[memIndex][i]);
					}
					java.lang.System.out.println("Set the cluster to " + memberships[memIndex]);
				}
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public void endR()
	{
		java.lang.System.setSecurityManager(curManager);
		RConnection retCon = (RConnection)retrieveVariable(R_CONN);
		try {
			if(retCon != null) {
				retCon.shutdown();
			}
			// clean up other things
			removeVariable(R_CONN);
			removeVariable(R_PORT);
			System.out.println("R Shutdown!!");
			java.lang.System.setSecurityManager(reactorManager);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void initR(int port)
	{
		RSingleton.getConnection(port);
	}
	
	@Override
	protected void cleanUpR() {
		// introducing this method to clean up everything 
		// remove all the stored variables
		// clean up R connection
		endR();
		// cleanup the directory
		java.lang.System.setSecurityManager(curManager);
		File file = new File(fileName);
		file.delete();
		file = new File(wd);
		file.delete();
		java.lang.System.setSecurityManager(reactorManager);
	}
}
