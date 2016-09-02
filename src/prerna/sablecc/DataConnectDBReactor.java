package prerna.sablecc;

import java.util.Iterator;
import java.util.List;

import prerna.ds.nativeframe.NativeFrame;


public class DataConnectDBReactor  extends AbstractReactor{

	public DataConnectDBReactor() {
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM, PKQLEnum.EXPLAIN};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_CONNECTDB;
	}
	
	@Override
	public Iterator process() {
		
		List engine = (List)myStore.get(PKQLEnum.WORD_OR_NUM);
		System.out.println(engine);
		
		NativeFrame frame = (NativeFrame)myStore.get("G");
		frame.setConnection(engine.get(0).toString().trim());

		return null;
	}

	@Override
	public String explain() {
		// TODO Auto-generated method stub
		String msg = "";
		msg += "DataConnectDBReactor";
		return msg;
	}

}
