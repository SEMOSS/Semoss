package prerna.sablecc;

import java.util.Iterator;

import prerna.engine.api.IConstructStatement;
import prerna.engine.api.IEngine;
import prerna.om.GraphDataModel;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.DIHelper;

public class GDMImportDataReactor extends ImportDataReactor{

	@Override
	public Iterator process() {
		String nodeStr = (String)myStore.get(whoAmI);
		
		GraphDataModel gdm = (GraphDataModel) myStore.get("G");
		gdm.setOverlay(true);
		
		Iterator<IConstructStatement> it = null;
		IEngine engine = null;
		if(myStore.containsKey(PKQLEnum.API)) {
			it  = (Iterator) myStore.get(PKQLEnum.API);
			String engineName = (String) myStore.get(PKQLEnum.API + "_" + "ENGINE");
			engine = (IEngine) DIHelper.getInstance().getLocalProp(engineName);
		} else {
			// TODO: nope, doing nothing
			myStore.put("STATUS", STATUS.ERROR);
			myStore.put(nodeStr, "Data maker is GraphDataModel which doesn't allow this type of addition");
			return null;
		}
		
		// do the overlay
		gdm.overlayData(it, engine);
		// this is actually what pushes the info from the rc in gdm into the vert and edge stores
		gdm.fillStoresFromModel(engine);
		gdm.updateDataId();

		inputResponseString(it, null);
		myStore.put("STATUS", STATUS.SUCCESS);
		
		return null;
	}
}