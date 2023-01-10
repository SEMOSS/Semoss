package prerna.sablecc2.reactor.frame;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.py.PandasFrame;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser2;
import prerna.query.querystruct.GenExpression;
import prerna.query.querystruct.filters.AndQueryFilter;
import prerna.query.querystruct.filters.IQueryFilter;
import prerna.query.querystruct.filters.OrQueryFilter;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;




public class NLPQuery2Reactor extends AbstractReactor {

	// get a NLP Text
	// starts the environment / sets the model
	// convert text to sql through pipeline
	// plug the pipeline into insight
	
	//
	private static final Logger logger = LogManager.getLogger(NLPQueryReactor.class);

	public NLPQuery2Reactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.TOKEN_COUNT.getKey()};
	}

	
	@Override
	public NounMetadata execute() {
		
		if(!(this.insight.getCurFrame() instanceof PandasFrame))
			return NounMetadata.getErrorNounMessage("NLP Query 2 has only been implemented for python at this point, please convert your frames to python and try again");

		
		organizeKeys();
		String query = keyValue.get(keysToGet[0]);
		
		int maxTokens = 150;
		if(keyValue.containsKey(keysToGet[1]))
		{
			maxTokens = Integer.parseInt(keyValue.get(keysToGet[1]));
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
		Set <ITableDataFrame> allFrames = this.insight.getVarStore().getFrames();
				
		StringBuffer finalDbString = new StringBuffer("### SQL Tables, with their properties:");
		finalDbString.append("\\n#\\n");
		
		Iterator <ITableDataFrame> frameIterator = allFrames.iterator();
		while(frameIterator.hasNext())
		{
			ITableDataFrame thisFrame = frameIterator.next();
			logger.info("Processing frame " + thisFrame.getName());
			finalDbString.append("#").append(thisFrame.getName()).append("(");
			
			// Getting the column values routine. not doing this for now
			/*
			HashMap columnValues = new HashMap();
			if(thisFrame instanceof PandasFrame)
			{
				Object output = this.insight.getPyTranslator().runScript(thisFrame.getName() + "w.get_categorical_values()");
				if(output instanceof HashMap)
					columnValues = (HashMap)this.insight.getPyTranslator().runScript(thisFrame.getName() + "w.get_categorical_values()");
			}
		
			finalDbString.append(" | ").append(thisFrame.getName()).append(" : ");
			*/
			String [] columns = thisFrame.getColumnHeaders();
			
			// if the frame is pandas frame get the data
			// we will get to this shortly
			for(int columnIndex = 0;columnIndex < columns.length;columnIndex++)
			{
				if(columnIndex == 0)
					finalDbString.append(columns[columnIndex]);
				else
					finalDbString.append(" , ").append(columns[columnIndex]);
				//if(columnValues.containsKey(columns[columnIndex]))
				//	finalDbString.append(columnValues.get(columns[columnIndex]));
			}
			finalDbString.append(")\\n");
		}

		finalDbString.append("#\\n").append("### A query to list ").append(query).append("\\n").append("SELECT");
		
		logger.info("executing query " + finalDbString);

		Object output = insight.getPyTranslator().runScript("smssutil.run_gpt_3(\"" + finalDbString + "\", " + maxTokens + ")");
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
		String frameMaker = frameName + "= pd.DataFrame(sqldf(\"" + sqlDFQuery + "\"))";
		logger.info("Creating frame with query..  " + sqlDFQuery + " <<>> " + frameMaker);
		insight.getPyTranslator().runEmptyPy("from pandasql import sqldf");
		insight.getPyTranslator().runScript(frameMaker); // load the sql df
		
		// check to see if the variable was created
		// if not this is a bad query
		boolean frameCreated = (Boolean)insight.getPyTranslator().runScript("'" + frameName + "' in globals()");
		
		List<NounMetadata> outputs = new Vector<NounMetadata>(1);
	
				
		if(frameCreated)
		{
			// now we just need to tell the user here is the frame
			String frameType = "Py";
			StringBuffer outputString = new StringBuffer("Query Generated : " + sqlDFQuery + "\nData : " + frameName);
			outputString.append("\n");
			outputString.append(this.insight.getPyTranslator().runSingle(insight.getUser().getVarMap(), frameName + ".head(20)"));
			outputString.append("\n");
			outputString.append("To start working with this frame  GenerateFrameFrom" + frameType + "Variable('" + frameName + "')");
			
			outputs.add(new NounMetadata(outputString.toString(), PixelDataType.CONST_STRING));
			
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		}
		else
		{
			outputs.add(new NounMetadata("Could not compute the result / query invalid -- \n" + sqlDFQuery, PixelDataType.CONST_STRING));
			return new NounMetadata(outputs, PixelDataType.CODE, PixelOperationType.CODE_EXECUTION);
		}
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
