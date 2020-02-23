package prerna.sablecc2.reactor.frame;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.om.Insight;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;
import prerna.sablecc2.reactor.insights.copy.CopyFrameUtil;

public class CacheNativeFrame extends AbstractFrameReactor {

	private static final String CLASS_NAME = CacheNativeFrame.class.getName();
	private static final String SYNCHRONIZE = "inThread";

	public CacheNativeFrame() {
		this.keysToGet = new String[] {ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.FRAME_TYPE.getKey(), SYNCHRONIZE};
	}

	@Override
	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);
		ITableDataFrame frame = getFrame();
		if(!(frame instanceof NativeFrame)) {
			throw new IllegalArgumentException("Frame must be a native frame");
		}
		
		String frameType = getFrameType();
		ITableDataFrame newFrame = null;
		try {
			String alias = frame.getName() + "_OLD";
			newFrame = FrameFactory.getFrame(this.insight, frameType, alias);
			NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
			this.insight.getVarStore().put(alias, noun);
		} catch (Exception e) {
			throw new IllegalArgumentException("Error occured trying to create frame of type " + frameType, e);
		}
		
		CachingThread cache = new CachingThread();
		cache.insight = this.insight;
		cache.frame = (NativeFrame) frame;
		cache.newFrame = newFrame;
		cache.logger = logger;
		if(inThread()) {
			cache.run();
			return new NounMetadata(true, PixelDataType.BOOLEAN);
		} else {
			java.lang.Thread t = new Thread(cache);
			t.start();
			return new NounMetadata("Swapping frame in parallel thread", PixelDataType.CONST_STRING, PixelOperationType.OPERATION);
		}
	}

	/**
	 * Get the frame type
	 * @return
	 */
	private String getFrameType() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		
		return this.curRow.get(0).toString();
	}
	
	/**
	 * True means run in the current thread
	 * False means run in its own thread
	 * @return
	 */
	private boolean inThread() {
		GenRowStruct grs = this.store.getNoun(this.keysToGet[2]);
		if(grs != null && !grs.isEmpty()) {
			List<Object> booleanValues = grs.getValuesOfType(PixelDataType.BOOLEAN);
			if(booleanValues != null && !booleanValues.isEmpty()) {
				return (boolean) booleanValues.get(0);
			}
		}
		
		List<Object> booleanValues = this.curRow.getValuesOfType(PixelDataType.BOOLEAN);
		if(booleanValues != null && !booleanValues.isEmpty()) {
			return (boolean) booleanValues.get(0);
		}
		
		return false;
	}
	

	class CachingThread implements Runnable {

		private Insight insight;
		private NativeFrame frame;
		private ITableDataFrame newFrame;
		private Logger logger;
		
		@Override
		public void run() {
			// query all the data in the qs
			// we will not merge the frame filters since we want to add that as 
			// the state filters at the end
			SelectQueryStruct qs = frame.getMetaData().getFlatTableQs(true);
			qs.setFrame(frame);
			IRawSelectWrapper it = frame.query(qs);
			IImporter importer = ImportFactory.getImporter(newFrame, qs, it);
			importer.insertData();
			
			Set<String> f1Keys = new HashSet<String>();
			Set<String> f2Keys = new HashSet<String>();

			// loop through and identify all the alias for frame 1 and frame 2
			VarStore vStore = insight.getVarStore();
			for(String key : vStore.getKeys()) {
				NounMetadata n = vStore.get(key);
				if(n.getNounType() == PixelDataType.FRAME) {
					if(n.getValue() == frame) {
						// is it f1
						f1Keys.add(key);
					} else if(n.getValue() == newFrame) {
						// is it f2
						f2Keys.add(key);
					}
				}
			}
			
			// now set the filters of the current frame to the new frame
			logger.info("Transfering " + frame.getFrameFilters().size() + " filters to cached frame");
			CopyFrameUtil.copyFrameFilters(frame, newFrame);
			
			// need to also change the frame names
			// need to account for R and stuff
			String frame1Name = frame.getName();
			String frame2Name = newFrame.getName();
						
			logger.info("Swapping " + (f1Keys.size() + f2Keys.size()) + " keys");
			NounMetadata f1Noun = new NounMetadata(frame, PixelDataType.FRAME);
			NounMetadata f2Noun = new NounMetadata(newFrame, PixelDataType.FRAME);
			// now swap
			for(String f2 : f2Keys) {
				// here we put the f1Noun
				vStore.put(f2, f1Noun);
			}
			CopyFrameUtil.renameFrame(newFrame, frame1Name);
			
			for(String f1 : f1Keys) {
				// here we put the f2Noun
				vStore.put(f1, f2Noun);
			}
			CopyFrameUtil.renameFrame(frame, frame2Name);
		}
	}

}


