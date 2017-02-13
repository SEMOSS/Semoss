package prerna.sablecc;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

import org.apache.commons.io.FileUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPString;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.TinkerFrame;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class BaseJavaReactorJRI extends AbstractRJavaReactor {

	public BaseJavaReactorJRI() {
		super();
	}
	
	public BaseJavaReactorJRI(ITableDataFrame frame) {
		super(frame);
	}

	/**
	 * Starts the connection to R
	 */
	@Override
	protected Object startR() {
		Rengine retEngine = Rengine.getMainEngine();
		LOGGER.info("Connection right now is set to.. " + retEngine);
		if(retEngine == null) {
			try {
				// start the R Engine
				retEngine = new Rengine(null, true, null);
				LOGGER.info("Successfully created engine.. ");

				// load all the libraries
				retEngine.eval("library(splitstackshape);");
				// data table
				retEngine.eval("library(data.table);");
				// reshape2
				retEngine.eval("library(reshape2);");
				// rjdbc
				retEngine.eval("library(RJDBC);");
				storeVariable(R_ENGINE, retEngine);
			} catch (Exception e) {
				System.out.println("ERROR ::: Could not find connection.\nPlease make sure RServe is running and the following libraries are installed:\n"
						+ "1)splitstackshape\n"
						+ "2)data.table\n"
						+ "3)reshape2"
						+ "4)RJDBC\n\n"
						+ "Please note RJDBC might require JAVA_HOME environment path to be defined on your system.");
				e.printStackTrace();
			}
		}
		return retEngine;
	}
	
	/**
	 * Execute a rScript
	 */
	public Object eval(String script)
	{
		Rengine engine = (Rengine)startR();
		try {
			REXP rexp = engine.eval(script);
			if(rexp == null) {
				LOGGER.info("Hmmm... REXP returned null for script = " + script);
			}
			return rexp;
		} catch (Exception e) {
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
		Rengine retEngine = (Rengine)startR();
		try {
			// get the articulation points
			int [] vertices = retEngine.eval("articulation.points(" + graphName + ")").asIntArray();
			// now for each vertex get the name
			Hashtable <String, String> dataHash = new Hashtable<String, String>();
			for(int vertIndex = 0;vertIndex < vertices.length;  vertIndex++)
			{
				String output = retEngine.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\", " + vertices[vertIndex] + ")").asString();
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		System.out.println(" Key Nodes \n " + names);
	}
	
	public void colorClusters(String clusterName)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		Rengine retEngine = (Rengine)startR();
		// the color is saved as color
		try
		{
			double [] memberships = retEngine.eval(clusterName + "$membership").asDoubleArray();
			String [] IDs = retEngine.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")").asStringArray();
			
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
			org.rosuda.REngine.RList list = ((REXPGenericVector)output).asList();
			
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
			org.rosuda.REngine.RList list = ((REXPList)output).asList();
			
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
		
		// JRI
		else if(output instanceof org.rosuda.JRI.REXP) {
			int typeInt = ((org.rosuda.JRI.REXP) output).getType();
			if(typeInt == REXP.XT_DOUBLE) {
				builder.append( ((org.rosuda.JRI.REXP) output).asDouble()  );
				
			} else if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asDoubleArray() ));

			} else if(typeInt == REXP.XT_ARRAY_DOUBLE + 1) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asDoubleMatrix() ));

			} else if(typeInt == REXP.XT_INT) {
				builder.append( ((org.rosuda.JRI.REXP) output).asInt()  );
				
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asIntArray() ));
				
			} else if(typeInt == REXP.XT_STR) {
				builder.append( ((org.rosuda.JRI.REXP) output).asString()  );
				
			} else if(typeInt == REXP.XT_ARRAY_STR) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asStringArray() ));
				
			} else if(typeInt == REXP.XT_BOOL) {
				builder.append( ((org.rosuda.JRI.REXP) output).asBool()  );
				
			} else if(typeInt == REXP.XT_ARRAY_BOOL) {
				
			} else if(typeInt == REXP.XT_LIST) {
				builder.append( ((org.rosuda.JRI.REXP) output).asString()  );
				
			} else if(typeInt == REXP.XT_VECTOR) {
				builder.append(Arrays.toString( ((org.rosuda.JRI.REXP) output).asStringArray() ));
				
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
		String tempFileLocation = DIHelper.getInstance().getProperty(Constants.INSIGHT_CACHE_DIR) + "\\" + DIHelper.getInstance().getProperty(Constants.CSV_INSIGHT_CACHE_FOLDER);
		tempFileLocation += "\\" + Utility.getRandomString(15) + ".R";
		tempFileLocation = tempFileLocation.replace("\\", "/");
		
		File f = new File(tempFileLocation);
		try {
			FileUtils.writeStringToFile(f, script);
		} catch (IOException e1) {
			System.out.println("Error in writing R script for execution!");
			e1.printStackTrace();
		}

		Rengine retEngine = (Rengine)startR();
		try {
			REXP output = retEngine.eval("paste( capture.output(print( source(\"" + tempFileLocation + "\")$value ) ), collapse='\n')");
			if(output.getType() == REXP.XT_NULL) {
				throw new IllegalArgumentException("Unable to wrap method in paste/capture");
			}
			if(result) {
				System.out.println(output.asString());
			}
		} catch(Exception e) {
			try {
				Object output = retEngine.eval(script);
				if(result)
				{
					java.lang.System.out.println("RCon data.. " + output);
					StringBuilder builder = new StringBuilder();
					getResultAsString(output, builder);
					System.out.println("Output : " + builder.toString());
				}
			} catch (Exception e2) {
				e2.printStackTrace();
				String errorMessage = null;
				if(e2.getMessage() != null && !e2.getMessage().isEmpty()) {
					errorMessage = e2.getMessage();
				} else {
					errorMessage = "Unexpected error in execution of R routine ::: " + script;
				}
				throw new IllegalArgumentException(errorMessage);
			}
		} finally {
			f.delete();
		}
	}
	
	// split a column based on a value
	public void performSplitColumn(String frameName, String columnName, String separator, boolean dropColumn, boolean frameReplace)
	{
		//  cSplit(dt, "PREFIX", "_")
		Rengine engine = (Rengine)startR();
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
			eval(script);
			System.out.println("Script " + script);
			// get all the columns that are factors
			script = "sapply(" + tempName + ", is.factor);";
			int [] factors = engine.eval(script).asIntArray();			
			String [] colNames = getColNames(tempName);
			
			// now I need to compose a string based on it
			String conversionString = "";
			for(int factorIndex = 0;factorIndex < factors.length;factorIndex++)
			{
				if(factors[factorIndex] == 1) // this is a factor
				{
					conversionString = conversionString + 
							tempName + "$" + colNames[factorIndex] + " <- "
							+ "as.character(" + tempName + "$" +colNames[factorIndex] + ");";
				}
			}
			engine.eval(conversionString);
			engine.eval(frameReplaceScript);
			
			System.out.println("Script " + script);
			System.out.println("Complete ");
			// once this is done.. I need to find what the original type is and then apply a type to it
			// may be as string
			// or as numeric
			// else this is not giving me what I want :(
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
		
	public String[] getColNames(String frameName, boolean print)
	{
		Rengine engine = (Rengine)startR();
		String [] colNames = null;
		try {
				String script = "names(" + frameName + ");";
				colNames = engine.eval(script).asStringArray();
				if(print)
				{
					System.out.println("Columns..");
					for(int colIndex = 0;colIndex < colNames.length;colIndex++)
						System.out.println(colNames[colIndex] + "\n");
				}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return colNames;
	}
	
	public String getColType(String frameName, String colName, boolean print) {
		Rengine engine = (Rengine)startR();
		String colType = null;
		try {
			String script = "sapply(" + frameName + "$" + colName + ", class);";
			colType = engine.eval(script).asString();
			if(print) {
				System.out.println(colName + "has type " + colType);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return colType;
	}
	
	public String[] getColTypes(String frameName, boolean print)
	{
		Rengine engine = (Rengine)startR();
		String [] colTypes = null;
		try {
			String script = "sapply(" + frameName + ", class);";
			colTypes = engine.eval(script).asStringArray();
			if(print) {
				System.out.println("Columns..");
				for(int colIndex = 0; colIndex < colTypes.length; colIndex++) {
					System.out.println(colTypes[colIndex] + "\n");
				}
			}
		} catch (Exception e) {
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
		Rengine engine = (Rengine)startR();
		Object [][] retOutput = null; // name and the number of items
		
		try {
			String script = "";
			
			String [] colNames = getColNames(frameName, false);
			// I am not sure if I need the colnames right now but...
			
			// get the column types
			String [] colTypes = getColTypes(frameName, false);
			
			// get the blank columns
			script = "" + frameName + "[, colSums( " + frameName + " != \"\") !=0]";
			script = "sapply(" + frameName + ", function (k) all(is.na(k)))"; // all the zeros are non empty
			REXP rexp = engine.eval(script);
			int [] blankCols = rexp.asIntArray();
			
			Hashtable <String, Integer> colCount = new Hashtable <String, Integer>();
			
			for(int colIndex = 0;colIndex < colTypes.length;colIndex++)
			{
				String colType = colTypes[colIndex];
				if(blankCols[colIndex] == 1)
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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	public void performJoinColumns(String frameName, String newColumnName,  String separator, String cols)
	{
		// reconstruct the column names
		//paste(df1$a_1, df1$a_2, sep="$$")
		try {
			Rengine engine = (Rengine)startR();
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
			engine.eval(script);
			System.out.println("Join Complete ");
		
		} catch (Exception e) {
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
		Rengine engine = (Rengine)startR();
		Object [][] retOutput = null; // name and the number of items
		
		try {
			String script = "colData <-  " + frameName + "[, .N, by=\"" + column +"\"];";
			System.out.println("Script is " + script);
			engine.eval(script);
			script = "colData$" + column;
			String [] uniqueColumns = engine.eval(script).asStringArray();
			// need to limit this eventually to may be 10-15 and no more
			script = "matrix(colData$N);"; 
			int [] colCount = engine.eval(script).asIntArray();
			int total = 0;
			retOutput = new Object[uniqueColumns.length][2];
			StringBuilder builder = null;
			if(print) {
				builder = new StringBuilder();
				builder.append(column + "\t Count \n");
			}
			for(int outputIndex = 0;outputIndex < uniqueColumns.length;outputIndex++)
			{
				retOutput[outputIndex][0] = uniqueColumns[outputIndex];
				retOutput[outputIndex][1] = colCount[outputIndex];
				total += colCount[outputIndex];
				if(print) {
					builder.append(retOutput[outputIndex][0] + "\t" + retOutput[outputIndex][1] + "\n");
				}
			}
			if(print) {
				builder.append("===============\n");
				builder.append("Total \t " + total);
				System.out.println("Output : " + builder.toString());
			} else {
				// create the weird object the FE needs to paint a bar chart
				this.returnData = getBarChartInfo(column, "Frequency", retOutput);
				this.hasReturnData = true;
			}
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return retOutput;
	}
	
	@Override
	protected int getNumRows(String frameName) {
		Rengine rcon = (Rengine)startR();
		return rcon.eval("nrow(" + frameName + ")").asInt();
	}
	
	public Object[][] getDescriptiveStats(String frameName, String column, boolean print) {
		// start the R first
		Rengine rcon = (Rengine)startR();
		Object [][] retOutput = new Object[8][2]; // name and the number of items
		String frameExpr = frameName + "$" + column;
		String script = "min(as.numeric(na.omit(" + frameExpr + ")))";
		double min = rcon.eval(script).asDouble();
		retOutput[0][0] = "Minimum";
		retOutput[0][1] = min;

		script = "quantile(" + frameExpr + ", prob = c(0.25, 0.75))";
		double[] quartiles = rcon.eval(script).asDoubleArray();
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
		return retOutput;
	}
	
	public Object[][] getHistogram(String frameName, String column, int numBreaks, boolean print) {
		Rengine rcon = (Rengine)startR();

		String script = null;
		if(numBreaks > 1) {
			script = "hist(" + frameName + "$" + column + ", breaks=" + numBreaks + ", plot=FALSE)";
		} else {
			script = "hist(" + frameName + "$" + column + ", plot=FALSE)";
		}
		System.out.println("Script is " + script);
		REXP histR = rcon.eval(script);
		// this comes back as a vector
		RVector vectorR = histR.asVector();

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
		double[] breaks = vectorR.at("breaks").asDoubleArray();
		int[] counts = vectorR.at("counts").asIntArray();
		int numBins = counts.length;
		Object[][] data = new Object[numBins][2];

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
		
		return data;
	}
	
	public void unpivot(String frameName, String cols, boolean replace)
	{
		// makes the columns and converts them into rows
		// need someway to indicate to our metadata as well that this is gone
		//melt(dat, id.vars = "FactorB", measure.vars = c("Group1", "Group2"))
		
		String concatString = "";
		startR();
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
		try {
			// modifying this to execute single script at a time
			String script = tempName + "<- melt(" + frameName + concatString + ")" ;
			eval(script);
//			script = tempName + " <- " + tempName + "[,lapply(.SD, as.character)];";
//			eval(script);
			script = replacer;
			eval(script);
			System.out.println("executing script " + script);
			if(replace && checkRTableModified(frameName)) {
				recreateMetadata(frameName);
			}
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}

	public void pivot(String frameName, boolean replace,String columnToPivot, String cols)
	{
		// makes the columns and converts them into rows
		
		//dcast(molten, formula = subject~ variable)
		// I need columns to keep and columns to pivot
		
		startR();
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
		
		String script = newFrame + " <- dcast(" + frameName + keepString + ");";
		eval(script);
		script = replaceString ;
		eval(script);
		System.out.println("Completed Pivoting..");
		if(replace && checkRTableModified(frameName)) {
			recreateMetadata(frameName);
		}
	}
	
	public void synchronizeXY(String rVariable)
	{
		String graphName = (String)retrieveVariable("GRAPH_NAME");

		Rengine retEngine = (Rengine)startR();
		try
		{
			double [][] memberships = retEngine.eval("xy_layout").asDoubleMatrix();
			String [] axis = null;
			if(memberships[0].length == 2) {
				axis = new String[]{"X", "Y"};
			} else if(memberships[0].length == 3) {
				axis = new String[]{"X", "Y", "Z"};
			}
			
			String [] IDs = retEngine.eval("vertex_attr(" + graphName + ", \"" + TinkerFrame.TINKER_ID + "\")").asStringArray();
			
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
		Rengine retCon = (Rengine)retrieveVariable(R_ENGINE);
		try {
			if(retCon != null) {
				retCon.end();
			}
			// clean up other things
			removeVariable(R_ENGINE);
			removeVariable(R_PORT);
			System.out.println("R Shutdown!!");
			java.lang.System.setSecurityManager(reactorManager);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
