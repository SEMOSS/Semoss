package prerna.query.querystruct;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.query.parsers.GenExpressionWrapper;
import prerna.query.parsers.SqlParser2;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class SQLSetParamsReactor extends AbstractReactor
{
	public SQLSetParamsReactor()
	{
		// id _type can be column, column_table, colum_table_operator
		this.keysToGet = new String[] { ReactorKeysEnum.ID.getKey(), ReactorKeysEnum.VALUE.getKey(), ReactorKeysEnum.ID_TYPE.getKey()};
	}

	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		organizeKeys();
		String id = keyValue.get(keysToGet[0]);
		Object value = keyValue.get(keysToGet[1]);
		
		String type = "column";
		if(keyValue.containsKey(keysToGet[2]))
			type = keyValue.get(keysToGet[2]);
		
		Object obj = insight.getVar(SQLGetParamsReactor.QS_WRAPPER);
		String query = "No such id found";
		ITableDataFrame frame = insight.getCurFrame();
		SelectQueryStruct sqs = null;
		if(frame != null && frame instanceof NativeFrame)
			sqs = ((NativeFrame)frame).getQueryStruct(); //this.insight.getLastQS(insight.getLastPanelId());

		if(obj == null && sqs != null)
		{
			// may be the user is doing for first time create it
			String curQuery = sqs.getCustomFrom();

			SqlParser2 sqp2 = new SqlParser2();
			try {
				GenExpressionWrapper wrapper = sqp2.processQuery(curQuery);
				Object [] allColumns = wrapper.columnTableIndex.keySet().toArray();
				insight.getVarStore().put(SQLGetParamsReactor.QS_WRAPPER, new NounMetadata(wrapper, PixelDataType.CUSTOM_DATA_STRUCTURE));
				obj = wrapper;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		if(obj != null)
		{
			GenExpressionWrapper wrapper = (GenExpressionWrapper)obj;
			
			if(type.equalsIgnoreCase("column"))
				wrapper.replaceColumn(id, value);
			else if(type.equalsIgnoreCase("column_table"))
				wrapper.replaceTableColumn(id, value);
			if(type.equalsIgnoreCase("column_table_operator"))
				wrapper.replaceTableColumnOperator(id, value);
			query = "Parameters have been set";
		}
		return new NounMetadata(query, PixelDataType.CONST_STRING);
	}
	
	
	
}