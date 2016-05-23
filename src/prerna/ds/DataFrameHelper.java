package prerna.ds;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import prerna.algorithm.api.IMetaData;
import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.H2.H2Frame;
import prerna.engine.api.ISelectStatement;
import prerna.engine.api.ISelectWrapper;

public class DataFrameHelper {

	public static void removeData(ITableDataFrame frame, ISelectWrapper it) {
		if(frame instanceof H2Frame) {
			
			while(it.hasNext()){
				ISelectStatement ss = (ISelectStatement) it.next();
				System.out.println(((ISelectStatement)ss).getPropHash());
				frame.removeRelationship(ss.getPropHash(), ss.getRPropHash());
			}
			
		} else if(frame instanceof TinkerFrame) {
			
			IMetaData metaData = ((TinkerFrame)frame).metaData;
			String[] columnHeaders = frame.getColumnHeaders();
			H2Frame tempFrame = TableDataFrameFactory.convertToH2Frame(frame);
			while(it.hasNext()){
				ISelectStatement ss = (ISelectStatement) it.next();
				System.out.println(((ISelectStatement)ss).getPropHash());
				tempFrame.removeRelationship(ss.getPropHash(), ss.getRPropHash());
			}
			
			TinkerFrame tframe = new TinkerFrame();
			tframe.metaData = metaData;
			Iterator<Object[]> iterator = tempFrame.iterator(false);
			while(iterator.hasNext()) {
				Map<String, Object> nextRow = new HashMap<String, Object>();
				Object[] row = iterator.next();
				for(int i = 0; i < row.length; i++) {
					nextRow.put(columnHeaders[i], row[i]);
				}
				tframe.addRelationship(nextRow, nextRow);
			}
			((TinkerFrame) frame).g= tframe.g;
		}
	}
}
