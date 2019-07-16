package prerna.sablecc;

import java.util.Iterator;

import prerna.ds.rdbms.h2.H2Frame;
import prerna.sablecc.PKQLEnum.PKQLReactor;
import prerna.sablecc.meta.IPkqlMetadata;

public class ConnectReactor extends AbstractReactor {

	// this is responsible for doing the replacing of variables in the script
	// with their actual objects
	// the map of vars to objects will either sit on runner or in insight
	// for now we will put it on the insight since runner isn't singleton wrt
	// insight

	public ConnectReactor() {
		String[] thisReacts = { PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLReactor.NETWORK_CONNECT.toString();
	}

	@Override
	public Iterator process() {

		// for now I would just connect the frame
		// eventually, I need to get to the point where I pick the table name and do the username / password thing
		H2Frame frame = (H2Frame)myStore.get("G");		
		if (myStore.get("TABLE_NAME") != null) {
			String tableName = (String)myStore.get(PKQLEnum.WORD_OR_NUM.toString());
			String [] userPass = frame.createUser(tableName);
			myStore.put("USERNAME", userPass[0]);
			myStore.put("PASSWORD", userPass[1]);
		}
		String url = frame.connectFrame();
		myStore.put("RESPONSE", url);
		return null;
	}
	
	@Override // no need for any sets.. 
	public void set(String key, Object value) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

}
