package prerna.reactor.frame.gaas;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser2;
import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.HardSelectQueryStruct;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NLPQuery3Reactor extends AbstractFrameReactor {

	// get a NLP Text
	// starts the environment / sets the model
	// convert text to sql through pipeline
	// plug the pipeline into insight

	//
	private static final Logger classLogger = LogManager.getLogger(NLPQuery3Reactor.class);

	public NLPQuery3Reactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.COMMAND.getKey(), "json", ReactorKeysEnum.TOKEN_COUNT.getKey(),
				ReactorKeysEnum.FRAME.getKey(), "allFrames", "dialect", ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String query = keyValue.get(keysToGet[0]);

		boolean json = true;
		if (keyValue.containsKey(keysToGet[1])) {
			if (keyValue.get(keysToGet[1]).equalsIgnoreCase("true")) {
				json = true;
			} else {
				json = false;
			}
		}
		int maxTokens = 150;
		if (keyValue.containsKey(keysToGet[2])) {
			maxTokens = Integer.parseInt(keyValue.get(keysToGet[2]));
		}

		List<ITableDataFrame> theseFrames = new ArrayList<>();
		if (Boolean.parseBoolean(this.keyValue.get(this.keysToGet[4]))) {
			theseFrames.addAll(this.getAllFrames());
			if (theseFrames.isEmpty()) {
				return NounMetadata.getErrorNounMessage("No frames found");
			}
		} else {
			ITableDataFrame thisFrame = getFrameDefaultLast();
			if (thisFrame == null) {
				return NounMetadata.getErrorNounMessage("No frame found for " + keyValue.get(keysToGet[3]));
			}
			theseFrames.add(thisFrame);
		}

		String dialect = this.keyValue.get(this.keysToGet[5]);
		if (dialect == null || (dialect = dialect.trim()).isEmpty()) {
			dialect = "SQLite3";
		}

		IModelEngine engine = null;

		if (keyValue.containsKey(keysToGet[6])) {
			String engineId = this.keyValue.get(this.keysToGet[6]);
			engine = (IModelEngine) Utility.getEngine(engineId);
		}

		if (engine == null) {
			String engineId = DIHelper.getInstance().getProperty(Constants.SQL_MOOSE_MODEL);
			engine = (IModelEngine) Utility.getEngine(engineId);
		}
		
		if(engine == null) {
			throw new IllegalArgumentException("Model engine ID must be passed in or added as a property");
		}
		
		List<NounMetadata> retListForFrames = new ArrayList<>();

		for (ITableDataFrame thisFrame : theseFrames) {
//			StringBuffer finalDbString = new StringBuffer();
			StringBuffer finalDbString2 = new StringBuffer();
//			StringBuffer finalQuery = new StringBuffer();
//			finalDbString.append("Given Database Schema: ");
			finalDbString2.append("You are tasked with generating sql to best answer a user's question given a table schema and the question. Below is both schema and the question, respond with the correct sql and ensure that the output starts and ends with ``` markdown. If user specifies any space delimeted value related to a column from dataset then make sure to replace the space with an underscore character.\n\nTABLE SCHEMA: ");
			Map<String, SemossDataType> columnTypes = thisFrame.getMetaData().getHeaderToTypeMap();
//			finalDbString.append("CREATE TABLE ").append(thisFrame.getName()).append("(");
			finalDbString2.append("CREATE TABLE ").append(thisFrame.getName()).append("(");
			Iterator<String> columns = columnTypes.keySet().iterator();
			while (columns.hasNext()) {
				String thisColumn = columns.next();
				SemossDataType colType = columnTypes.get(thisColumn);

				thisColumn = thisColumn.replace(thisFrame.getOriginalName() + "__", "");
				String colTypeString = SemossDataType.convertDataTypeToString(colType);
				if (colType == SemossDataType.DOUBLE || colType == SemossDataType.INT)
					colTypeString = "NUMBER";
				if (colType == SemossDataType.STRING)
					colTypeString = "TEXT";

//				finalDbString.append(thisColumn).append("  ").append(colTypeString).append(",");
				finalDbString2.append(thisColumn).append("  ").append(colTypeString).append(",");
			}
//			finalDbString.append(")");
//			finalDbString.append(". Provide an SQL to list ").append(query);
//			finalDbString.append(". Be Concise. Provide as markdown. Output should start and end with ``` markdown.");
//			
			finalDbString2.append(")\n\n");
			finalDbString2.append("USER QUESTION: ").append(query);
			finalDbString2.append("\n\nRespond with the correct sql and ensure that the output starts and ends with ``` markdown.");
			
//			classLogger.info(finalDbString + "");
			classLogger.info("prompt2: "+finalDbString2 + "");

			Object output = null;
			Map params = new HashMap();
			params.put("temperature", "0.3");
			Map<String, String> modelOutput = engine.ask(finalDbString2 + "", null, this.insight, params);
			String response = modelOutput.get("response");
			classLogger.info("Response: "+response);

			// if it comes in with finalDBString take it out
			response = response.replace(finalDbString2, "");

			String markdown = "```";
			int start = response.indexOf(markdown);
			if (start >= 0)
				response = response.substring(start + markdown.length());
			// get the select also
			start = response.indexOf("SELECT");
			if (start >= 0)
				response = response.substring(start);
			// remove the end quotes
			int end = response.indexOf("```");
			if (end >= 0)
				response = response.substring(0, end);
			end = response.indexOf(";");
			if (end >= 0)
				response = response.substring(0, end);
			classLogger.info(response);
			output = response;
//			}
			// get the string
			// make a frame
			// load the frame into insight
			classLogger.info("SQL query is " + output);

			// Create a new SQL Data Frame
			String sqlDFQuery = output.toString().trim();
			// remove the new line
			sqlDFQuery = sqlDFQuery.replace("\n", " ");
			sqlDFQuery = sqlDFQuery.replaceAll("[\\t\\n\\r]+"," ");
			classLogger.info("sql df query: "+sqlDFQuery);

			// execute sqlDF to create a frame
			// need to check if the query is right and then feed this into sqldf

			// need to parse this
			// a. see if the table names match with the frame names if not change it
			// b. See the constants and change the value based on the appropriate value the
			// column has - you can circumvent this by giving value in quotes

			String frameName = Utility.getRandomString(5);

			Map<String, String> outputMap = new HashMap<>();

			boolean sameColumns = isSameColumns(sqlDFQuery, thisFrame);
			outputMap.put("COLUMN_CHANGE", sameColumns + "");

			if (thisFrame instanceof PandasFrame) {
				sqlDFQuery = sqlDFQuery.replace("\"", "\\\"");

				// do we need a way to check the library is installed?

				PandasFrame pFrame = (PandasFrame) thisFrame;
				String sqliteName = pFrame.getSQLite();

				// pd.read_sql("select * from diab1 where age > 60", conn)
				String frameMaker = frameName + " = pd.read_sql(\"" + sqlDFQuery + "\", " + sqliteName + ")";
				classLogger.info("Creating frame with query..  " + sqlDFQuery + " <<>> " + frameMaker);
				insight.getPyTranslator().runEmptyPy(frameMaker);
				String sampleOut = insight.getPyTranslator().runSingle(insight.getUser().getVarMap(),
						frameName + ".head(20)", this.insight); // load the sql df

				System.err.println(sampleOut);
				// send information
				// check to see if the variable was created
				// if not this is a bad query

				if (sampleOut != null && sampleOut.length() > 0) {
					if (json) {
						outputMap.put(ReactorKeysEnum.FRAME_TYPE.getKey(), DataFrameTypeEnum.PYTHON.getTypeAsString());
						outputMap.put("Query", sqlDFQuery);
						outputMap.put(ReactorKeysEnum.FRAME.getKey(), frameName);
						outputMap.put("SAMPLE", sampleOut);
						outputMap.put("COMMAND", "GenerateFrameFromPyVariable('" + frameName + "')");
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						outputString.append("\nData : " + frameName);
						outputString.append("\n");
						outputString.append(sampleOut);
						outputString.append("\n");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				} else {
					if (json) {
						outputMap.put("Query", sqlDFQuery);
						outputMap.put("SAMPLE", "Could not compute data, query is not correct.");
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						outputString.append("\n");
						outputString.append("Query did not yield any results... ");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
					try {
						this.insight.getPyTranslator().runScript("del " + frameName + " , sqldf");
					} catch (Exception ignored) {

					}
				}
			} else if (thisFrame instanceof RDataTable) {
				sqlDFQuery = sqlDFQuery.replace("\"", "\\\"");
				AbstractRJavaTranslator rt = insight.getRJavaTranslator(this.getClass().getName());
				rt.checkPackages(new String[] { "sqldf" });

				String frameMaker = frameName + " <- sqldf(\"" + sqlDFQuery + "\")";
				classLogger.info("Creating frame with query..  " + sqlDFQuery + " <<>> " + frameMaker);
				rt.runRAndReturnOutput("library(sqldf)");
				rt.runR(frameMaker); // load the sql df

				boolean frameCreated = rt.runRAndReturnOutput("exists('" + frameName + "')").toUpperCase()
						.contains("TRUE");

				if (frameCreated) {
					String sampleOut = rt.runRAndReturnOutput("head(" + frameName + ", 20)");
					if (json) {
						outputMap.put(ReactorKeysEnum.FRAME_TYPE.getKey(), DataFrameTypeEnum.R.getTypeAsString());
						outputMap.put("Query", sqlDFQuery);
						outputMap.put(ReactorKeysEnum.FRAME.getKey(), frameName);
						outputMap.put("SAMPLE", sampleOut);
						outputMap.put("COMMAND", "GenerateFrameFromRVariable('" + frameName + "')");
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						// now we just need to tell the user here is the frame
						outputString.append("\nData : " + frameName);
						outputString.append("\n");
						outputString.append(sampleOut);
						outputString.append("\n");
						outputString.append(
								"To start working with this frame  GenerateFrameFromRVariable('" + frameName + "')");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				} else {
					if (json) {
						outputMap.put("Query", sqlDFQuery);
						outputMap.put("SAMPLE", "Could not compute data, query is not correct.");
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						outputString.append("\n");
						outputString.append("Query did not yield any results... ");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				}
			} else if (thisFrame instanceof NativeFrame) {
				// we do a query from a subquery
				SelectQueryStruct allDataQs = thisFrame.getMetaData().getFlatTableQs(true);
				String baseQuery = ((NativeFrame) thisFrame).getEngineQuery(allDataQs);
				String newQuery = sqlDFQuery.replace(thisFrame.getName(),
						"(" + baseQuery + ") as " + thisFrame.getName());

				HardSelectQueryStruct hqs = new HardSelectQueryStruct();
				hqs.setQuery(newQuery);
				int counter = 0;
				List<List<Object>> sampleOut = new ArrayList<>();
				try {
					IRawSelectWrapper it = thisFrame.query(hqs);
					while (it.hasNext() && counter < 10) {
						sampleOut.add(Arrays.asList(it.next().getValues()));
						counter++;
					}

					if (json) {
						outputMap.put(ReactorKeysEnum.FRAME_TYPE.getKey(), DataFrameTypeEnum.NATIVE.getTypeAsString());
						outputMap.put("Query", newQuery);
						outputMap.put(ReactorKeysEnum.FRAME.getKey(), frameName);
						outputMap.put("SAMPLE", sampleOut.toString());

						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + newQuery);
						outputString.append("\nData : " + frameName);
						outputString.append("\n");
						outputString.append(sampleOut);
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				} catch (Exception e) {
					outputMap.put("Query", newQuery);
					outputMap.put("SAMPLE", "Could not compute data, query is not correct.");
					if (json) {
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + newQuery);
						outputString.append("\n");
						outputString.append("Query did not yield any results... ");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				}
			} else if (thisFrame instanceof AbstractRdbmsFrame) {
				HardSelectQueryStruct hqs = new HardSelectQueryStruct();
				hqs.setQuery(sqlDFQuery);
				int counter = 0;
				List<List<Object>> sampleOut = new ArrayList<>();
				try {
					IRawSelectWrapper it = thisFrame.query(hqs);
					while (it.hasNext() && counter < 10) {
						sampleOut.add(Arrays.asList(it.next().getValues()));
						counter++;
					}

					if (json) {
						outputMap.put(ReactorKeysEnum.FRAME_TYPE.getKey(), DataFrameTypeEnum.GRID.getTypeAsString());
						outputMap.put("Query", sqlDFQuery);
						outputMap.put(ReactorKeysEnum.FRAME.getKey(), frameName);
						outputMap.put("SAMPLE", sampleOut.toString());

						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						outputString.append("\nData : " + frameName);
						outputString.append("\n");
						outputString.append(sampleOut);
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				} catch (Exception e) {
					outputMap.put("Query", sqlDFQuery);
					outputMap.put("SAMPLE", "Could not compute data, query is not correct.");
					if (json) {
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						outputString.append("\n");
						outputString.append("Query did not yield any results... ");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				}
			} else {
				retListForFrames.add(getError(
						"NLP Query 3 has only been implemented for python, r, grid, and native frame at this point, please convert your frames to python,r and try again"));
			}
		}

		return new NounMetadata(retListForFrames, PixelDataType.VECTOR, PixelOperationType.VECTOR);
	}

	private boolean isSameColumns(String sqlDFQuery, ITableDataFrame thisFrame) {
		boolean sameColumns = true;
		try {
			SqlParser2 p2 = new SqlParser2();
			GenExpressionWrapper wrapper = p2.processQuery(sqlDFQuery);

			String[] columnHeaders = thisFrame.getColumnHeaders();
			boolean allColumns = false;

			List<GenExpression> selects = wrapper.root.nselectors;
			if (selects.size() == 1) {
				// possibly select *
				GenExpression allSelect = selects.get(0);
				allColumns = allSelect.getLeftExpr().equalsIgnoreCase("*");
				// we are good
			}
			if (!allColumns) {
				for (int selectorIndex = 0; selectorIndex < columnHeaders.length && sameColumns; selectorIndex++) // going
																													// to
																													// run
																													// a
																													// dual
																													// for
																													// loop
																													// here
				{
					String thisColumn = columnHeaders[selectorIndex];
					boolean foundThisColumn = false;
					for (int newColumnIndex = 0; newColumnIndex < selects.size(); newColumnIndex++) {
						GenExpression thisSelector = selects.get(newColumnIndex);
						String alias = thisSelector.getLeftAlias();
						if (alias == null)
							alias = thisSelector.getLeftExpr();
						if (thisColumn.equalsIgnoreCase(alias))
							foundThisColumn = true;
					}
					sameColumns = sameColumns & foundThisColumn;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			classLogger.info(e.getMessage());
			;
			sameColumns = false;
		}
		return sameColumns;
	}	
}