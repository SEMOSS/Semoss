package prerna.sablecc;

import java.util.Hashtable;
import java.util.Iterator;

public class VizReactor extends AbstractReactor {


	Hashtable <String, String[]> values2SyncHash = new Hashtable <String, String[]>();
	
	public VizReactor()
	{
		String [] thisReacts = {};
		super.whatIReactTo = thisReacts;
		super.whoAmI = PKQLEnum.VIZ;
	}
	
	@Override
	public Iterator process() {
		// TODO Auto-generated method stub
		return null;
	}

}
