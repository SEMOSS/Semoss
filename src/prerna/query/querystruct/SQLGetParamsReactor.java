package prerna.query.querystruct;

import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser2;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;


public class SQLGetParamsReactor extends AbstractReactor
{
	public static final String QS_WRAPPER = "QS_WRAPPER";
	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		// get the query struct
		// parse it
		// get the query params
		// set it into the insight
		
		SelectQueryStruct sqs = this.insight.getLastQS(insight.getLastPanelId());
		NounMetadata retData = null;
		GenExpressionWrapper wrapper = null;
		
		Object obj = insight.getVar(QS_WRAPPER);
		
		if(obj == null)
		{
			// handling for the hard query struct only for now
			// need to see if we want to do the same for others ?
			if(sqs.getCustomFrom() != null)
			{		
				//HardSelectQueryStruct hsqs = (HardSelectQueryStruct)sqs;
				String query = sqs.getCustomFrom();
				SqlParser2 sqp2 = new SqlParser2();
				try {
					wrapper = sqp2.processQuery(query);
					Object [] allColumns = wrapper.columnTableIndex.keySet().toArray();
					retData = new NounMetadata(allColumns, PixelDataType.VECTOR);
					insight.getVarStore().put(QS_WRAPPER, new NounMetadata(wrapper, PixelDataType.CUSTOM_DATA_STRUCTURE));
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		else
		{
			wrapper = (GenExpressionWrapper)obj;
			Object [] allColumns = wrapper.columnTableIndex.keySet().toArray();
			retData = new NounMetadata(allColumns, PixelDataType.VECTOR);			
		}
		
		return retData;
	}
	
	
	
}