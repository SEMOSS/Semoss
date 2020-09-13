package prerna.sablecc;

import java.util.Iterator;

import prerna.ds.nativeframe.NativeFrame;
import prerna.sablecc.meta.IPkqlMetadata;

public class DataConnectDBReactor  extends AbstractReactor{

	public DataConnectDBReactor() {
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_CONNECTDB;
	}
	
	@Override
	public Iterator process() {
		String engine = (String)myStore.get(PKQLEnum.WORD_OR_NUM);
		System.out.println(engine);
		
		NativeFrame frame = (NativeFrame)myStore.get("G");
		frame.setConnection(engine.trim());

		return null;
	}

	@Override
	public IPkqlMetadata getPkqlMetadata() {
		// TODO Auto-generated method stub
		return null;
	}
}
