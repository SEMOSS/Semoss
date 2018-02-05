package prerna.sablecc2.reactor.runtime;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.rosuda.JRI.REXP;
import org.rosuda.JRI.RFactor;
import org.rosuda.JRI.RVector;
import org.rosuda.JRI.Rengine;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPGenericVector;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPList;
import org.rosuda.REngine.REXPString;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractBaseJriRImpl extends AbstractBaseRClass {
	
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

		try {
			REXP output = (REXP) this.rJavaTranslator.executeR("paste( capture.output(print( source(\"" + tempFileLocation + "\")$value ) ), collapse='\n')");
			if(output.getType() == REXP.XT_NULL) {
				throw new IllegalArgumentException("Unable to wrap method in paste/capture");
			}
			if(result) {
				System.out.println(output.asString());
			}
		} catch(Exception e) {
			try {
				Object output = this.rJavaTranslator.executeR(script);
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

	// gives the different types of columns that are there and how many are there of that type
	// such as 5 integer columns
	// 3 string columns
	// 4 blank columns etc. 
	public void getColumnTypeCount(String frameName)
	{
		Object [][] retOutput = null; // name and the number of items
		
		try {
			String script = "";
			
			String [] colNames = this.rJavaTranslator.getColumns(frameName);
			// I am not sure if I need the colnames right now but...
			
			// get the column types
			String [] colTypes = this.rJavaTranslator.getColumnTypes(frameName);
			
			// get the blank columns
			script = "" + frameName + "[, colSums( " + frameName + " != \"\") !=0]";
			script = "sapply(" + frameName + ", function (k) all(is.na(k)))"; // all the zeros are non empty
			REXP rexp = (REXP) this.rJavaTranslator.executeR(script);
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
	
	public Object[][] getColumnCount(String frameName, String column)
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
		Object [][] retOutput = null; // name and the number of items
		
		try {
			String tempName = Utility.getRandomString(6);
			String script = tempName + " <-  " + frameName + "[, .N, by=\"" + column +"\"];";
			System.out.println("Script is " + script);
			this.rJavaTranslator.executeR(script);
			
			// get the column names
			String colType = this.rJavaTranslator.getColumnType(frameName, column);
			script = tempName + "$" + column;

			String[] uniqueColumns = this.rJavaTranslator.getStringArray(script);
			if (colType.equalsIgnoreCase("string") || colType.equalsIgnoreCase("factor")
					|| colType.equalsIgnoreCase("character")) {
				if (uniqueColumns == null) {
					RFactor factors = (RFactor) this.rJavaTranslator.getFactor(script);
					int numFactors = factors.size();
					uniqueColumns = new String[numFactors];
					for (int i = 0; i < numFactors; i++) {
						uniqueColumns[i] = factors.at(i);
					}
				}
			} else {
				getHistogram(frameName, column, 0);
				return null;
			}
			
			// need to limit this eventually to may be 10-15 and no more
			script = "matrix(" + tempName + "$N);"; 
			int [] colCount = this.rJavaTranslator.getIntArray(script);
			retOutput = new Object[uniqueColumns.length][2];
			for(int outputIndex = 0;outputIndex < uniqueColumns.length;outputIndex++) {
				retOutput[outputIndex][0] = uniqueColumns[outputIndex];
				retOutput[outputIndex][1] = colCount[outputIndex];
			}
			// create and return a task
			Map<String, Object> taskData = getBarChartInfo(column, "Frequency", retOutput);
			this.nounMetaOutput.add(new NounMetadata(taskData, PixelDataType.TASK, PixelOperationType.TASK_DATA));

			//variable cleanup
			this.rJavaTranslator.executeR("rm(" + tempName + "); gc();");

		} catch (Exception e) {
			e.printStackTrace();
		} 
		return retOutput;
	}

	public Object[][] getColumnCount(String frameName, String column, boolean top)
	{
		Object [][] retOutput = null; // name and the number of items

		try {
			String tempName = Utility.getRandomString(6);
			String script = tempName + " <-  " + frameName + "[, .N, by=\"" + column +"\"];";
			System.out.println("Script is " + script);
			this.rJavaTranslator.executeR(script);
			if(top) {
				this.rJavaTranslator.executeR(tempName + " <- " + tempName + "[order(-rank(N)),]");
			} else {
				this.rJavaTranslator.executeR(tempName + " <- " + tempName + "[order(rank(N)),]");
			}

			// get the column names
			String colType = this.rJavaTranslator.getColumnType(frameName, column);
			script = tempName + "$" + column;
			String[] uniqueColumns = this.rJavaTranslator.getStringArray(script);
			if (colType.equalsIgnoreCase("string") || colType.equalsIgnoreCase("factor")
					|| colType.equalsIgnoreCase("character")) {
				if (uniqueColumns == null) {
					RFactor factors = (RFactor) this.rJavaTranslator.getFactor(script);
					int numFactors = factors.size();
					uniqueColumns = new String[numFactors];
					for (int i = 0; i < numFactors; i++) {
						uniqueColumns[i] = factors.at(i);
					}
				}
			} else {
				getHistogram(frameName, column, 0);
				return null;
			}
			// get the count for each column
			script = tempName + "$N";
			int [] colCount = this.rJavaTranslator.getIntArray(script);

			// create the object with the right size
			if(uniqueColumns.length > 100) {
				retOutput = new Object[100][2];
			} else {
				retOutput = new Object[uniqueColumns.length][2];
			}

			int counter = 0;
			for(int outputIndex = 0;outputIndex < uniqueColumns.length && counter < 100; outputIndex++) {
				retOutput[outputIndex][0] = uniqueColumns[outputIndex];
				retOutput[outputIndex][1] = colCount[outputIndex];
				counter++;
			}

			// create and return a task
			Map<String, Object> taskData = getBarChartInfo(column, "Frequency", retOutput);
			this.nounMetaOutput.add(new NounMetadata(taskData, PixelDataType.TASK, PixelOperationType.TASK_DATA));

			//variable cleanup
			this.rJavaTranslator.executeR("rm(" + tempName + "); gc();");

		} catch (Exception e) {
			e.printStackTrace();
		} 
		return retOutput;
	}
	
	public Object[][] getDescriptiveStats(String frameName, String column) {
		Object [][] retOutput = new Object[8][2]; // name and the number of items
		String frameExpr = frameName + "$" + column;
		String script = "min(as.numeric(na.omit(" + frameExpr + ")))";
		double min = this.rJavaTranslator.getDouble(script);
		retOutput[0][0] = "Minimum";
		retOutput[0][1] = min;

		script = "quantile(as.numeric(na.omit(" + frameExpr + ")), prob = c(0.25, 0.75))";
		double[] quartiles = this.rJavaTranslator.getDoubleArray(script);
		retOutput[1][0] = "Q1";
		retOutput[1][1] = quartiles[0];
		retOutput[2][0] = "Q3";
		retOutput[2][1] = quartiles[1];
		
		script = "max(as.numeric(na.omit(" + frameExpr + ")))";
		double max = this.rJavaTranslator.getDouble(script);
		retOutput[3][0] = "Maximum";
		retOutput[3][1] = max;

		script = "mean(as.numeric(na.omit(" + frameExpr + ")))";
		double mean = this.rJavaTranslator.getDouble(script);
		retOutput[4][0] = "Mean";
		retOutput[4][1] = mean;

		script = "median(as.numeric(na.omit(" + frameExpr + ")))";
		double median = this.rJavaTranslator.getDouble(script);
		retOutput[5][0] = "Median";
		retOutput[5][1] = median;

		script = "sum(as.numeric(na.omit(" + frameExpr + ")))";
		double sum = this.rJavaTranslator.getDouble(script);
		retOutput[6][0] = "Sum";
		retOutput[6][1] = sum;

		script = "sd(as.numeric(na.omit(" + frameExpr + ")))";
		double sd = this.rJavaTranslator.getDouble(script);
		retOutput[7][0] = "Standard Deviation";
		retOutput[7][1] = sd;

		Map<String, Object> taskData = getBarChartInfo(column, "StatOutput", retOutput);
		this.nounMetaOutput.add(new NounMetadata(taskData, PixelDataType.TASK, PixelOperationType.TASK_DATA));

		return retOutput;
	}
	
	public Object[][] getHistogram(String frameName, String column, int numBreaks) {
		String script = null;
		if(numBreaks > 1) {
			script = "hist(" + frameName + "$" + column + ", breaks=" + numBreaks + ", plot=FALSE)";
		} else {
			script = "hist(" + frameName + "$" + column + ", plot=FALSE)";
		}
		System.out.println("Script is " + script);
		REXP histR = (REXP) this.rJavaTranslator.executeR(script);
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

		for(int i = 0; i < numBins; i++) {
			data[i][0] = breaks[i] + " - " + breaks[i+1];
			data[i][1] = counts[i];
		}
		
		Map<String, Object> taskData = getBarChartInfo(column, "Frequency", data);
		this.nounMetaOutput.add(new NounMetadata(taskData, PixelDataType.TASK, PixelOperationType.TASK_DATA));
		
		return data;
	}
	
	@Override
	protected Map<String, Object> flushObjectAsTable(String framename, String[] colNames) {
		List<Object[]> dataMatrix = new ArrayList<Object[]>();
		
		int numCols = colNames.length;
		for (int i = 0; i < numCols; i++) {
			String script = framename + "$" + colNames[i];
			REXP val = (REXP) this.rJavaTranslator.executeR(script);
			int typeInt = val.getType();
			
			if(typeInt == REXP.XT_ARRAY_DOUBLE) {
				double[] rows = val.asDoubleArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if(typeInt == REXP.XT_ARRAY_INT) {
				int[] rows = val.asIntArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if (typeInt == REXP.XT_ARRAY_STR) {
				String[] rows = val.asStringArray();
				int numRows = rows.length;
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, numRows, numCols);
				}
				for(int j = 0; j < numRows; j++) {
					dataMatrix.get(j)[i] = rows[j];
				}
			} else if(typeInt == REXP.XT_INT) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asInt();
			} else if(typeInt == REXP.XT_DOUBLE) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asDouble();
			} else if(typeInt == REXP.XT_STR) {
				if(dataMatrix.isEmpty()) {
					initEmptyMatrix(dataMatrix, 1, numCols);
				}
				dataMatrix.get(0)[i] = val.asString();
			}
		}
		
		Map<String, Object> retMap = new HashMap<String, Object>();
		retMap.put("headers", colNames);
		retMap.put("data", dataMatrix);
		
		return retMap; 
	}
	
	private void initEmptyMatrix(List<Object[]> matrix, int numRows, int numCols) {
		for(int i = 0; i < numRows; i++) {
			matrix.add(new Object[numCols]);
		}
	}

	@Override
	protected void endR() {
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
	
	// split a column based on a value
    @Override
    public void performSplitColumn(String frameName, String[] columnNames, String separator, String direction,
            boolean dropColumn, boolean frameReplace) {
        // cSplit(dt, "PREFIX", "_")
        // need to get the type of this
        try {
            String tempName = Utility.getRandomString(8);

            String frameReplaceScript = frameName + " <- " + tempName + ";";
            if (!frameReplace) {
                frameReplaceScript = "";
            }
            String columnReplaceScript = "TRUE";
            if (!dropColumn) {
                columnReplaceScript = "FALSE";
            }
            if (direction == null || direction.isEmpty()) {
                direction = "wide";
            }
            for (String columnName : columnNames) {
                String script = tempName + " <- cSplit(" + frameName + ", " + "\"" + columnName + "\", \"" + separator
                        + "\", direction = \"" + direction + "\", drop = " + columnReplaceScript + ");";
                this.rJavaTranslator.executeR(script);
                System.out.println("Script " + script);
                // get all the columns that are factors
                script = "sapply(" + tempName + ", is.factor);";
                int[] factors = this.rJavaTranslator.getIntArray(script);
                String[] colNames = this.rJavaTranslator.getColumns(tempName);

                // now I need to compose a string based on it
                String conversionString = "";
                for (int factorIndex = 0; factorIndex < factors.length; factorIndex++) {
                    if (factors[factorIndex] == 1) // this is a factor
                    {
                        conversionString = conversionString + tempName + "$" + colNames[factorIndex] + " <- "
                                + "as.character(" + tempName + "$" + colNames[factorIndex] + ");";
                    }
                }
                this.rJavaTranslator.executeR(conversionString);
                this.rJavaTranslator.executeR(frameReplaceScript);

                // perform variable cleanup
                this.rJavaTranslator.executeR("rm(" + tempName + "); gc();");
                System.out.println("Script " + script);
                System.out.println("Complete ");
            }
            // once this is done.. I need to find what the original type is and
            // then apply a type to it
            // may be as string
            // or as numeric
            // else this is not giving me what I want :(
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
