package prerna.sablecc;

import java.util.Iterator;

import prerna.ds.TableDataFrameFactory;
import prerna.ds.h2.H2Frame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IEngine;
import prerna.engine.api.IScriptReactor;
import prerna.sablecc.PKQLRunner.STATUS;
import prerna.util.DIHelper;

public class NativeImportDataReactor extends ImportDataReactor {

	public Iterator process() {
		super.process();

		if(myStore.containsKey(PKQLEnum.API)) {	
			NativeFrame frame = (NativeFrame)myStore.get("G");
			IEngine engine = (IEngine) DIHelper.getInstance().getLocalProp((this.getValue(PKQLEnum.API + "_ENGINE")+"").trim());

			String frameEngine = frame.getEngineName();
			if(frameEngine != null && engine != null) {
				//if we have data coming from a different engine, i.e. traversing across dbs
				if(!frameEngine.equals(engine.getEngineName())) {
					//need to convert from native frame to h2 frame
					H2Frame newFrame = TableDataFrameFactory.convertToH2FrameFromNativeFrame(frame);
					myStore.put(PKQLEnum.G, newFrame);

					String reactorName = newFrame.getScriptReactors().get(PKQLEnum.IMPORT_DATA);
					try {
						IScriptReactor importReactor = (IScriptReactor)Class.forName(reactorName).newInstance();
						return processScriptReactor(importReactor);
					} catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
						e.printStackTrace();
					}
				}
			}
		}

		myStore.put("STATUS", STATUS.SUCCESS);
		return null;
	}

	/**
	 * This is to auto-convert to H2Frame if there are table joins present within the table
	 * @param reactor
	 * @return
	 */
	private Iterator processScriptReactor(IScriptReactor reactor) {
		//set data into the reactor
		for(String key : myStore.keySet()) {
			reactor.put(key, myStore.get(key));
		}

		if(reactor instanceof H2ImportDataReactor) {
			((H2ImportDataReactor)reactor).process();
		}

		//set my data with data coming from the reactor
		for(String retKey : reactor.getKeys()) {
			myStore.put(retKey, reactor.getValue(retKey));
		}

		return null;
	}
}
