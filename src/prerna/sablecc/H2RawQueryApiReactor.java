package prerna.sablecc;

import java.util.Iterator;

import prerna.ds.h2.H2Frame;
import prerna.engine.api.IHeadersDataRow;
import prerna.sablecc.meta.IPkqlMetadata;

public class H2RawQueryApiReactor extends RawQueryApiReactor {

	public static final String QUERY_KEY = "QUERY";

	@Override
	public Iterator process() {
		H2Frame frame = (H2Frame) myStore.get("G");
		String query = (String) myStore.get(QUERY_KEY);
		
		// since the user doesn't know the table name
		// as it is dynamically generated
		// we will replace a placeholder for it with the frame name
		query = query.replace("<TABLE_NAME>", frame.getName());
		
		Iterator<IHeadersDataRow> it = frame.query(query);
		
		this.put((String) getValue(PKQLEnum.RAW_API), it);
		this.put("RESPONSE", "success");
		this.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		return null;
	}
}