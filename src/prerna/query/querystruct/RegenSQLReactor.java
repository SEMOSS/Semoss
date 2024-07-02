package prerna.query.querystruct;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;


public class RegenSQLReactor extends AbstractReactor
{
	private static final Logger classLogger = LogManager.getLogger(RegenSQLReactor.class);

	public RegenSQLReactor()
	{
		// id _type can be column, column_table, colum_table_operator
	}

	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		Object obj = insight.getVar(SQLGetParamsReactor.QS_WRAPPER);
		String query = "No such id found";
		ITableDataFrame frame = insight.getCurFrame();
		SelectQueryStruct sqs = null;
		
		if(frame != null && frame instanceof NativeFrame)
		{
			GenExpressionWrapper wrapper = (GenExpressionWrapper)obj;			
			try {
				wrapper.fillParameters();
				query = wrapper.printOutput();
				sqs = ((NativeFrame)frame).getQueryStruct();
				sqs.setCustomFrom(query);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				classLogger.error(Constants.STACKTRACE, e);
			}
		}
		return new NounMetadata(query, PixelDataType.CONST_STRING);
	}
	
	
	
}