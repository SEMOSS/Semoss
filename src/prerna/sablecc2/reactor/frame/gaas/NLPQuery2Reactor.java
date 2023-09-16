package prerna.sablecc2.reactor.frame.gaas;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.DataFrameTypeEnum;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.ds.py.PandasFrame;
import prerna.ds.r.RDataTable;
import prerna.ds.rdbms.AbstractRdbmsFrame;
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
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.frame.r.util.AbstractRJavaTranslator;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class NLPQuery2Reactor extends AbstractFrameReactor {

	// get a NLP Text
	// starts the environment / sets the model
	// convert text to sql through pipeline
	// plug the pipeline into insight

	//
	private static final Logger logger = LogManager.getLogger(NLPQuery2Reactor.class);

	public NLPQuery2Reactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.COMMAND.getKey(), 
				"json", 
				ReactorKeysEnum.TOKEN_COUNT.getKey(), 
				ReactorKeysEnum.FRAME.getKey(),
				"allFrames",
				"dialect"
		};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String query = keyValue.get(keysToGet[0]);

		boolean json = true;
		if(keyValue.containsKey(keysToGet[1])) {
			if(keyValue.get(keysToGet[1]).equalsIgnoreCase("true")) {
				json = true;
			} else {
				json = false;
			}
		}
		int maxTokens = 150;
		if(keyValue.containsKey(keysToGet[2])) {
			maxTokens = Integer.parseInt(keyValue.get(keysToGet[2]));
		}

		List<ITableDataFrame> theseFrames = new ArrayList<>();
		if(Boolean.parseBoolean(this.keyValue.get(this.keysToGet[4]))) {
			theseFrames.addAll(this.getAllFrames());
			if(theseFrames.isEmpty()) {
				return NounMetadata.getErrorNounMessage("No frames found");
			}
		} else {
			ITableDataFrame thisFrame = getFrameDefaultLast();
			if(thisFrame == null) {
				return NounMetadata.getErrorNounMessage("No frame found for " + keyValue.get(keysToGet[3]));
			}
			theseFrames.add(thisFrame);
		}

		String dialect = this.keyValue.get(this.keysToGet[5]);
		if(dialect == null || (dialect=dialect.trim()).isEmpty()) {
			dialect = "SQLite3";
		}

		
		String model = DIHelper.getInstance().getProperty(Constants.SQL_MOOSE_MODEL);
		if(model == null || model.trim().isEmpty()) {
			model = "gpt_3";
		}


		// create the prompt
		// format
		/*
		 * 
			### SQL tables, with their properties:
			#
			# Employee(id, name, department_id)
			# Department(id, name, address)
			# Salary_Payments(id, employee_id, amount, date)
			#
			### A query to list the names of employees in department sales living in virginia who make more than 10000
		 * 
		 */
		// use \n to separate

		// make the call to the open ai and get the response back

		// may be we should get all the frames here
		//Set <ITableDataFrame> allFrames = this.insight.getVarStore().getFrames();
		//Iterator <ITableDataFrame> frameIterator = allFrames.iterator();

		List<NounMetadata> retListForFrames = new ArrayList<>();

		for(ITableDataFrame thisFrame : theseFrames) 
		{
			StringBuffer finalDbString = new StringBuffer();
			StringBuffer finalQuery = new StringBuffer();
			if(model.equalsIgnoreCase("alpaca")) 
			{

				finalDbString.append("Below is an instruction that describes a task, paired with an input that provides further context. Write a response that appropriately completes the request.\\n\\n### Instruction:\\n");

				finalDbString.append(dialect+" SQL Tables, with their properties: \\n\\nTABLE Name ::: ");
				finalDbString.append(thisFrame.getName());
				finalDbString.append("\\n\\nCOLUMNS ::: ");
				String [] columns = thisFrame.getColumnHeaders();
				for(int columnIndex = 0;columnIndex < columns.length;columnIndex++) {
					if(columnIndex == 0) {
						finalDbString.append(columns[columnIndex]);
					} else { 
						finalDbString.append(" , ").append(columns[columnIndex]);
					}
				}
				finalDbString.append("\\n\\nWhat is the SQL to ").append(query);
				finalDbString.append("\\n\\n### Response:");
			} else if(model.equalsIgnoreCase("run_guanaco"))
			{
				finalDbString.append(dialect+" SQL Tables, with their properties: \\n\\nTABLE Name ::: ");
				finalDbString.append(thisFrame.getName());
				finalDbString.append("\\n\\nCOLUMNS ::: ");
				String [] columns = thisFrame.getColumnHeaders();
				for(int columnIndex = 0;columnIndex < columns.length;columnIndex++) {
					if(columnIndex == 0) {
						finalDbString.append(columns[columnIndex]);
					} else { 
						finalDbString.append(" , ").append(columns[columnIndex]);
					}
				}
				finalDbString.append("\\n\\nWhat is the SQL to ").append(query);
			}
			else if(model.equalsIgnoreCase("chat_guanaco"))
			{
				finalDbString.append("You are given the following tables and columns");
				finalDbString.append("\\n\\nTABLE:::").append(thisFrame.getName());
				finalDbString.append("\\n\\nCOLUMNS::: ");
				String [] columns = thisFrame.getColumnHeaders();
				for(int columnIndex = 0;columnIndex < columns.length;columnIndex++) {
					if(columnIndex == 0) {
						finalDbString.append(columns[columnIndex]);
					} else { 
						finalDbString.append(" , ").append(columns[columnIndex]);
					}
				}
				finalQuery.append("Provide as markdown. SQL to list ").append(query);				
			}
			else
			{
				finalDbString.append("### "+dialect+" SQL Tables, with their properties:");
				finalDbString.append("\\n#\\n");

				//ITableDataFrame thisFrame = frameIterator.next();
				logger.info("Processing frame " + thisFrame.getName());
				finalDbString.append("#").append(thisFrame.getName()).append("(");

				String [] columns = thisFrame.getColumnHeaders();

				// if the frame is pandas frame get the data
				// we will get to this shortly
				for(int columnIndex = 0;columnIndex < columns.length;columnIndex++) {
					if(columnIndex == 0) {
						finalDbString.append(columns[columnIndex]);
					} else { 
						finalDbString.append(" , ").append(columns[columnIndex]);
					}
				}
				finalDbString.append(")\\n");
				finalDbString.append("#\\n").append("### A query to list ").append(query).append("\\n").append("SELECT");

				logger.info("executing query " + finalDbString);

			}
			Object output;
			if(model.equalsIgnoreCase("alpaca")) {
				String endpoint = DIHelper.getInstance().getProperty(Constants.MOOSE_ENDPOINT);
				if(endpoint == null || endpoint.trim().isEmpty()) {
					throw new IllegalArgumentException("Must define endpoint to run custom models");
				} 
				output = insight.getPyTranslator().runScript("smssutil.run_alpaca(\"" + finalDbString + "\", " + maxTokens + " ,\" "  + endpoint.trim() + "\")");
			} else if(model.equalsIgnoreCase("run_guanaco"))
			{
				// create the client first
				
				// create instruction and get response back
				String endpoint = DIHelper.getInstance().getProperty(Constants.GUANACO_ENDPOINT);
				if(endpoint == null || endpoint.trim().isEmpty()) {
					throw new IllegalArgumentException("Must define endpoint to run custom models");
				} 
				output = insight.getPyTranslator().runScript("smssutil.run_guanaco(\"" + finalDbString + "\", " + maxTokens + " ,\""  + endpoint.trim() + "\")");
			}
			else if(model.equalsIgnoreCase("chat_guanaco")) {
				String endpoint = DIHelper.getInstance().getProperty(Constants.GUANACO_ENDPOINT);
				if(endpoint == null || endpoint.trim().isEmpty()) {
					throw new IllegalArgumentException("Must define endpoint to run custom models");
				} 
				maxTokens=500;
				// create the client class
				insight.getPyTranslator().runScript("from text_generation import Client");
				
				//String loginName = insight.getUser().getSingleLogginName(insight.getUser());
				
				String clientName = model + "_client";
				String hasClientName = "false";
				hasClientName = insight.getPyTranslator().runPyAndReturnOutput("print('" + clientName + "' in locals())");
				if(hasClientName.equalsIgnoreCase("false"))
				{
					insight.getPyTranslator().runScript(clientName + " = Client('" + endpoint + "')");				
					insight.getPyTranslator().runScript(clientName + ".timeout = 60");	
				}
				
				String command = "smssutil.chat_guanaco_code(context=\"" + finalDbString + "\", "
						  + "question=\"" + finalQuery + "\","
						  + "max_new_tokens="+ maxTokens + ","
						  + "client="  + clientName + ")";
				
				output = insight.getPyTranslator().runScript(command);
				
				if(output instanceof HashMap)
					output = ((HashMap)output).get("response");
				
				// replace the sql if there
				output = output.toString().replace("sql", "");
				//insight.getPyTranslator().runScript("del " + client_name);
			}
			else {
				output = insight.getPyTranslator().runScript("smssutil.run_gpt_3(\"" + finalDbString + "\", " + maxTokens + ")");
			}
			// get the string
			// make a frame
			// load the frame into insight
			logger.info("SQL query is " + output);

			//Create a new SQL Data Frame 
			String sqlDFQuery = output.toString().trim();
			// remove the new line
			sqlDFQuery = sqlDFQuery.replace("\n", " ");

			// execute sqlDF to create a frame
			// need to check if the query is right and then feed this into sqldf

			// need to parse this
			//a.  see if the table names match with the frame names if not change it 
			//b. See the constants and change the value based on the appropriate value the column has - you can circumvent this by giving value in quotes

			String frameName = Utility.getRandomString(5);

			Map<String, String> outputMap = new HashMap<>();

			boolean sameColumns = isSameColumns(sqlDFQuery, thisFrame);
			outputMap.put("COLUMN_CHANGE", sameColumns + "");

			if(thisFrame instanceof PandasFrame) {
				sqlDFQuery = sqlDFQuery.replace("\"", "\\\"");

				// do we need a way to check the library is installed?

				PandasFrame pFrame = (PandasFrame)thisFrame;
				String sqliteName = pFrame.getSQLite();

				// pd.read_sql("select * from diab1 where age > 60", conn)
				String frameMaker = "pd.read_sql(\"" + sqlDFQuery + "\", " + sqliteName + ").head(20)";
				logger.info("Creating frame with query..  " + sqlDFQuery + " <<>> " + frameMaker);
				String sampleOut = insight.getPyTranslator().runSingle(insight.getUser().getVarMap(), frameMaker, this.insight); // load the sql df

				System.err.println(sampleOut);
				// send information
				// check to see if the variable was created
				// if not this is a bad query

				if(sampleOut != null && sampleOut.length() > 0)
				{
					if(json) {
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
					try
					{
						this.insight.getPyTranslator().runScript("del " + frameName + " , sqldf");
					}catch(Exception ignored)
					{
						
					}
				}
				else
				{
					if(json) {
						outputMap.put("Query", sqlDFQuery);
						outputMap.put("SAMPLE", "Could not compute data, query is not correct.");
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						outputString.append("\n");
						outputString.append("Query did not yield any results... ");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));	
					}
					try
					{
						this.insight.getPyTranslator().runScript("del " + frameName + " , sqldf");
					}catch(Exception ignored)
					{
						
					}
				}
			}		
			else if (thisFrame instanceof RDataTable)
			{
				sqlDFQuery = sqlDFQuery.replace("\"", "\\\"");
				AbstractRJavaTranslator rt = insight.getRJavaTranslator(this.getClass().getName());
				rt.checkPackages(new String[] { "sqldf" });

				String frameMaker = frameName + " <- sqldf(\"" + sqlDFQuery + "\")";
				logger.info("Creating frame with query..  " + sqlDFQuery + " <<>> " + frameMaker);
				rt.runRAndReturnOutput("library(sqldf)");
				rt.runR(frameMaker); // load the sql df			

				boolean frameCreated = rt.runRAndReturnOutput("exists('" + frameName + "')").toUpperCase().contains("TRUE");

				if(frameCreated)
				{
					String sampleOut = rt.runRAndReturnOutput("head(" + frameName + ", 20)");
					if(json) {
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
						outputString.append("To start working with this frame  GenerateFrameFromRVariable('" + frameName + "')");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				}
				else
				{
					if(json) {
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
			}
			else if (thisFrame instanceof NativeFrame)
			{
				// we do a query from a subquery
				SelectQueryStruct allDataQs = thisFrame.getMetaData().getFlatTableQs(true);
				String baseQuery = ((NativeFrame) thisFrame).getEngineQuery(allDataQs);
				String newQuery = sqlDFQuery.replace(thisFrame.getName(), "(" + baseQuery+") as " + thisFrame.getName());

				HardSelectQueryStruct hqs = new HardSelectQueryStruct();
				hqs.setQuery(newQuery);
				int counter = 0;
				List<List<Object>> sampleOut = new ArrayList<>();
				try {
					IRawSelectWrapper it = thisFrame.query(hqs);
					while(it.hasNext() && counter < 10) {
						sampleOut.add(Arrays.asList(it.next().getValues()));
						counter++;
					}

					if(json) {
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
				} catch(Exception e) {
					outputMap.put("Query", newQuery);
					outputMap.put("SAMPLE", "Could not compute data, query is not correct.");
					if(json) {
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + newQuery);
						outputString.append("\n");
						outputString.append("Query did not yield any results... ");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				}
			}
			else if (thisFrame instanceof AbstractRdbmsFrame)
			{
				HardSelectQueryStruct hqs = new HardSelectQueryStruct();
				hqs.setQuery(sqlDFQuery);
				int counter = 0;
				List<List<Object>> sampleOut = new ArrayList<>();
				try {
					IRawSelectWrapper it = thisFrame.query(hqs);
					while(it.hasNext() && counter < 10) {
						sampleOut.add(Arrays.asList(it.next().getValues()));
						counter++;
					}

					if(json) {
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
				} catch(Exception e) {
					outputMap.put("Query", sqlDFQuery);
					outputMap.put("SAMPLE", "Could not compute data, query is not correct.");
					if(json) {
						retListForFrames.add(new NounMetadata(outputMap, PixelDataType.MAP));
					} else {
						StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery);
						outputString.append("\n");
						outputString.append("Query did not yield any results... ");
						retListForFrames.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
					}
				}
			}
			else
			{
				retListForFrames.add(getError("NLP Query 2 has only been implemented for python, r, grid, and native frame at this point, please convert your frames to python,r and try again"));
			}
		}

		return new NounMetadata(retListForFrames, PixelDataType.VECTOR, PixelOperationType.VECTOR);
	}

	private boolean isSameColumns(String sqlDFQuery, ITableDataFrame thisFrame)
	{
		boolean sameColumns = true;
		try 
		{
			SqlParser2 p2 = new SqlParser2();
			GenExpressionWrapper wrapper = p2.processQuery(sqlDFQuery);

			String [] columnHeaders = thisFrame.getColumnHeaders();
			boolean allColumns = false;

			List <GenExpression> selects = wrapper.root.nselectors;
			if(selects.size() == 1)
			{
				// possibly select *
				GenExpression allSelect = selects.get(0);
				allColumns = allSelect.getLeftExpr().equalsIgnoreCase("*");
				// we are good
			}
			if(!allColumns)
			{
				for(int selectorIndex = 0;selectorIndex < columnHeaders.length && sameColumns;selectorIndex++) // going to run a dual for loop here
				{
					String thisColumn = columnHeaders[selectorIndex];
					boolean foundThisColumn = false;
					for(int newColumnIndex = 0;newColumnIndex < selects.size();newColumnIndex++)
					{
						GenExpression thisSelector = selects.get(newColumnIndex);
						String alias = thisSelector.getLeftAlias();
						if(alias == null)
							alias = thisSelector.getLeftExpr();
						if(thisColumn.equalsIgnoreCase(alias))
							foundThisColumn = true;
					}
					sameColumns = sameColumns & foundThisColumn;
				}
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.info(e.getMessage());;
			sameColumns = false;
		}
		return sameColumns;
	}

	private void processSQL(String sql)
	{
		sql = "SELECT actor_name, title, gender" + 
				" FROM actor" + 
				" WHERE title = 'a' and gender > avg(age) " + 
				" and name > a+b and " +
				"title IN (SELECT title" + 
				"                FROM mv" + 
				"                WHERE director = 'Steven Spielberg' AND revenue_domestic > budget)" ;

		try 
		{
			SqlParser2 parser = new SqlParser2();
			parser.parameterize = false;
			GenExpressionWrapper gew = parser.processQuery(sql);

			// need to walk the gen expression and figure out if there are subqueries and start assimilating
			GenExpression ge = gew.root;

			// get the filter
			GenExpression mainFilter = ge.filter;

			Object finalObject = processFilter(mainFilter, null);
			// if main filter is not null
			// see what type of filter
			// if the operation is = then it is a simple filter
			// if it is AND/OR, it is an and filter and then parse again
			// if it is an IN filter then the right hand side could possibly be a query struct (like above)
			System.err.println(finalObject);


		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Object processFilter(GenExpression filter, IQueryFilter parentFilter)
	{

		// if main filter is not null
		// see what type of filter
		// if the operation is = then it is a simple filter
		// if it is AND/OR, it is an and filter and then parse again
		// if it is an IN filter then the right hand side could possibly be a query struct (like above)
		// or it is in list and then right side is an opaque list
		// else this is simple.. column and something else

		// other things to do
		// do paranthesis to find the levels - so it can all be added to the same step

		// if parentfilter == null
		// make parent filter
		// if not and this is not a paranthesis
		// (a or b) and (c) and ((e or f) and g)





		if(filter != null)
		{
			// if it is = or AND or OR
			if(filter.operation.equalsIgnoreCase("=") || filter.operation.equalsIgnoreCase(">")  || filter.operation.equalsIgnoreCase("<") || filter.operation.equalsIgnoreCase(">=")  
					|| filter.operation.equalsIgnoreCase("<=") 
					|| filter.operation.equalsIgnoreCase("AND") || filter.operation.equalsIgnoreCase("OR")

					|| filter.operation.equalsIgnoreCase("IN") // pass the left and right and get it aligned
					)
			{
				System.err.println("Operation " + filter.operation);

				System.err.println("Paranthesis " + filter.paranthesis);

				IQueryFilter newParentFilter = null;
				boolean root = false;
				if(parentFilter == null)
				{
					if(filter.operation.equalsIgnoreCase("AND"))
						parentFilter = new AndQueryFilter();
					else if (filter.operation.equalsIgnoreCase("OR"))
						parentFilter = new OrQueryFilter();

					else if((filter.operation.equalsIgnoreCase("=") || filter.operation.equalsIgnoreCase(">")  
							|| filter.operation.equalsIgnoreCase("<") || filter.operation.equalsIgnoreCase(">=")  
							|| filter.operation.equalsIgnoreCase("<="))
							|| filter.operation.equalsIgnoreCase("IN")
							)
					{
						NounMetadata replacer = new NounMetadata("replacer", PixelDataType.CONST_STRING);
						parentFilter = new SimpleQueryFilter(replacer, "***", replacer); // I will replace these values shortly.
					}

					root = true;
				}

				// if there is a paranthesis this is start of a new level
				if(filter.paranthesis && ! root) // start of a new world
				{
					if(filter.operation.equalsIgnoreCase("AND"))
						newParentFilter = new AndQueryFilter();
					else if (filter.operation.equalsIgnoreCase("OR"))
						newParentFilter = new OrQueryFilter();
					else if((filter.operation.equalsIgnoreCase("=") || filter.operation.equalsIgnoreCase(">")  
							|| filter.operation.equalsIgnoreCase("<") || filter.operation.equalsIgnoreCase(">=")  
							|| filter.operation.equalsIgnoreCase("<="))
							|| filter.operation.equalsIgnoreCase("IN")
							)
					{
						NounMetadata replacer = new NounMetadata("replacer", PixelDataType.CONST_STRING);
						newParentFilter = new SimpleQueryFilter(replacer, "***", replacer); // I will replace these values shortly.
					}

					if(parentFilter instanceof AndQueryFilter)
						((AndQueryFilter)parentFilter).addFilter(newParentFilter);

					if(parentFilter instanceof OrQueryFilter)
						((OrQueryFilter)parentFilter).addFilter(newParentFilter);

					parentFilter = newParentFilter;
				}

				// also address the regular case where this is not an and
				else 
					if((filter.operation.equalsIgnoreCase("=") || filter.operation.equalsIgnoreCase(">")  
							|| filter.operation.equalsIgnoreCase("<") || filter.operation.equalsIgnoreCase(">=")  
							|| filter.operation.equalsIgnoreCase("<="))
							|| filter.operation.equalsIgnoreCase("IN")
							)
					{
						NounMetadata replacer = new NounMetadata("replacer", PixelDataType.CONST_STRING);
						newParentFilter = new SimpleQueryFilter(replacer, "***", replacer); // I will replace these values shortly.

						if(parentFilter instanceof AndQueryFilter)
							((AndQueryFilter)parentFilter).addFilter(newParentFilter);

						if(parentFilter instanceof OrQueryFilter)
							((OrQueryFilter)parentFilter).addFilter(newParentFilter);

						parentFilter = newParentFilter;
					}

				Object leftItem = null;
				Object rightItem = null;
				if(filter.leftItem != null && filter.leftItem instanceof GenExpression)
				{
					GenExpression left = (GenExpression)filter.leftItem;
					// run the processing
					leftItem = processFilter(left, parentFilter);
				}
				if(filter.rightItem != null && filter.rightItem instanceof GenExpression)
				{
					GenExpression right = (GenExpression)filter.rightItem;
					// run the processing
					rightItem = processFilter(right, parentFilter);
				}

				// make the evaluation here to say what you want to do
				// if it is = then do those
				/*
				 * if(leftItem instanceof NounMetadata && rightItem instanceof NounMetadata &&
				 * parentFilter instanceof SimpleQueryFilter) { // this is the simple case
				 * SimpleQueryFilter sq = (SimpleQueryFilter)parentFilter;
				 * sq.reconstruct((NounMetadata)leftItem, filter.operation,
				 * (NounMetadata)rightItem); return sq; } else
				 */					return parentFilter;
			}

			// if it is a column print column
			if(filter.operation.equalsIgnoreCase("column"))
			{
				// this is a column convert and send it back
				NounMetadata nmd = new NounMetadata(filter.getLeftExpr(), PixelDataType.COLUMN);
				System.err.println(filter.leftItem);
				return nmd;
			}

			// see if it is a query struct
			if(filter.operation.equalsIgnoreCase("querystruct"))
			{
				// pick the body and send it back for processing
				// I dont know if we should process it and give it back as query struct but.. 
				NounMetadata nmd = new NounMetadata(filter.body.aQuery, PixelDataType.SUB_QUERY_EXPRESSION);
				System.err.println(filter.leftItem);
				return nmd;

			}

			if( filter.getOperation().equalsIgnoreCase("string"))
			{
				NounMetadata nmd = new NounMetadata(filter.leftItem, PixelDataType.CONST_STRING);
				System.err.println(filter.leftItem);
				return nmd;
			}
			if( filter.getOperation().equalsIgnoreCase("double"))
			{
				NounMetadata nmd = new NounMetadata(filter.getLeftExpr(), PixelDataType.CONST_DECIMAL);
				System.err.println(filter.leftItem);
				return nmd;
			}
			if( filter.getOperation().equalsIgnoreCase("date"))
			{
				NounMetadata nmd = new NounMetadata(filter.getLeftExpr(), PixelDataType.CONST_DATE);
				System.err.println(filter.leftItem);
				return nmd;
			}
			if( filter.getOperation().equalsIgnoreCase("time"))
			{
				NounMetadata nmd = new NounMetadata(filter.getLeftExpr(), PixelDataType.CONST_TIMESTAMP);
				System.err.println(filter.leftItem);
				return nmd;
			}
			if( filter.getOperation().equalsIgnoreCase("long"))
			{
				NounMetadata nmd = new NounMetadata(filter.leftItem, PixelDataType.CONST_INT);
				System.err.println(filter.leftItem);
				return nmd;
			}	
			else if(filter.getOperation().equalsIgnoreCase("function"))
			{
				// stop here please for function
				System.err.println("Function " + filter.operation);
			}
			else// this is opaque
			{
				System.err.println("something else " + filter.operation);				
			}
			// need to account for function

		}

		return null;
	}

	public static void main(String [] args)
	{
		NLPQuery2Reactor nl = new NLPQuery2Reactor();
		nl.processSQL(null);
	}


}
