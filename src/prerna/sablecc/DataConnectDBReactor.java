package prerna.sablecc;

import java.util.Iterator;


public class DataConnectDBReactor  extends AbstractReactor{

	public DataConnectDBReactor() {
		String [] thisReacts = {PKQLEnum.WORD_OR_NUM};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.DATA_CONNECTDB;
	}
	
	@Override
	public Iterator process() {
		
		Object engine = myStore.get(PKQLEnum.WORD_OR_NUM);
		System.out.println(engine);

		return null;
	}

}
