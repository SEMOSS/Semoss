package prerna.sablecc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import prerna.util.Constants;
import prerna.util.DIHelper;

public class DatabaseListReactor  extends AbstractReactor {

	public DatabaseListReactor() {
		super.whoAmI = PKQLEnum.DATABASE_LIST;
	}
	
	@Override
	public Iterator process() {
		String allEngines = DIHelper.getInstance().getLocalProp(Constants.ENGINES) + "";
		String[] engines = allEngines.split(";");
		
		Map<String, Object> result = new HashMap<String, Object>();
		List<String> enginesList = Arrays.asList(engines);
		
		result.put("engines", enginesList);
		
		myStore.put("database.list", result);
		myStore.put("STATUS", PKQLRunner.STATUS.SUCCESS);
		
		return null;
	}

}
