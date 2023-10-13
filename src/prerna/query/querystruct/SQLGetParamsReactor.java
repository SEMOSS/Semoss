package prerna.query.querystruct;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser2;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class SQLGetParamsReactor extends AbstractReactor
{
	public static final String QS_WRAPPER = "QS_WRAPPER";
	
	public SQLGetParamsReactor()
	{		// id _type can be column, column_table, colum_table_operator
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.ID_TYPE.getKey()};
	}
	
	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		// get the query struct
		// parse it
		// get the query params
		// set it into the insight
		organizeKeys();
		
		ITableDataFrame frame = insight.getCurFrame();
		SelectQueryStruct sqs = null;
		if(frame != null && frame instanceof NativeFrame)
			sqs = ((NativeFrame)frame).getQueryStruct(); //this.insight.getLastQS(insight.getLastPanelId());
		NounMetadata retData = null;
		GenExpressionWrapper wrapper = null;
		
		String id = keyValue.get(keysToGet[0]);
		
		String type = "column";
		if(keyValue.containsKey(keysToGet[1]))
			type = keyValue.get(keysToGet[1]);
				
		Object obj = insight.getVar(QS_WRAPPER);
		
		if(sqs != null && obj == null)
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
			//Object [] allColumns = wrapper.columnTableIndex.keySet().toArray();
			//retData = new NounMetadata(allColumns, PixelDataType.VECTOR);			
		}
		
		// finally give the result back
		if(type.equalsIgnoreCase("column") || id == null)
		{	
			Object [] allColumns = wrapper.columnTableIndex.keySet().toArray();
			retData = new NounMetadata(allColumns, PixelDataType.VECTOR);
		}
		if(type.equalsIgnoreCase("column_table"))
		{
			if(wrapper.columnTableIndex.containsKey(id))
			{
				Object [] allColumns = wrapper.columnTableIndex.get(id).toArray();
				retData = new NounMetadata(allColumns, PixelDataType.VECTOR);			
			}
		}
		if(type.equalsIgnoreCase("column_table_operator"))
		{
			if(wrapper.columnTableOperatorIndex.containsKey(id))
			{
				Object [] allColumns = wrapper.columnTableOperatorIndex.get(id).toArray();
				retData = new NounMetadata(allColumns, PixelDataType.VECTOR);			
			}
		}

		
		return retData;
	}
	
	
	
}