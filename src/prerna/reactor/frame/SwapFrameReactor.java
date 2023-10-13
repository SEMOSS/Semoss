package prerna.reactor.frame;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.reactor.AbstractReactor;
import prerna.reactor.insights.copy.CopyFrameUtil;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SwapFrameReactor extends AbstractReactor {

	private static final String CLASS_NAME = SwapFrameReactor.class.getName();

	public SwapFrameReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey()};
	}

	public NounMetadata execute() {
		Logger logger = getLogger(CLASS_NAME);

		List<ITableDataFrame> frames = getFramesToSwap();
		if(frames.size() != 2) {
			throw new IllegalArgumentException("Must have exactly 2 frames to swap");
		}

		Set<String> f1Keys = new HashSet<String>();
		Set<String> f2Keys = new HashSet<String>();

		// loop through and identify all the alias for frame 1 and frame 2
		VarStore vStore = this.insight.getVarStore();
		for(String key : vStore.getKeys()) {
			NounMetadata n = vStore.get(key);
			if(n.getNounType() == PixelDataType.FRAME) {
				if(n.getValue() == frames.get(0)) {
					// is it f1
					f1Keys.add(key);

				} else if(n.getValue() == frames.get(1)) {
					// is it f2
					f2Keys.add(key);

				}
			}
		}
		// now set the filters of the current frame to the new frame
		frames.get(1).setFilter(frames.get(0).getFrameFilters());
		
		// need to also change the frame names
		// need to account for R and stuff
		String frame1Name = frames.get(0).getName();
		String frame2Name = frames.get(1).getName();
				
		logger.info("Swapping " + (f1Keys.size() + f2Keys.size()) + " keys");
		NounMetadata f1Noun = new NounMetadata(frames.get(0), PixelDataType.FRAME);
		NounMetadata f2Noun = new NounMetadata(frames.get(1), PixelDataType.FRAME);
		// now swap
		for(String f1 : f1Keys) {
			// here we put the f2Noun
			vStore.put(f1, f2Noun);
		}
		CopyFrameUtil.renameFrame(frames.get(0), frame2Name);

		for(String f2 : f2Keys) {
			// here we put the f1Noun
			vStore.put(f2, f1Noun);
		}
		CopyFrameUtil.renameFrame(frames.get(1), frame1Name);

		return new NounMetadata(true, PixelDataType.BOOLEAN, PixelOperationType.FRAME_SWAP);
	}

	/**
	 * Get the frames to swap
	 * @return
	 */
	private List<ITableDataFrame> getFramesToSwap() {
		// either you passed them in cur row
		// or you passed them in using the frame
		List<ITableDataFrame> frames = new Vector<ITableDataFrame>();

		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			List<NounMetadata> nounList = grs.getNounsOfType(PixelDataType.FRAME);
			if(nounList != null) {
				for(int i = 0; i < nounList.size(); i++) {
					frames.add((ITableDataFrame) nounList.get(i).getValue());
				}
			}
		}

		List<NounMetadata> directNouns = this.curRow.getNounsOfType(PixelDataType.FRAME);
		if(directNouns != null) {
			for(int i = 0; i < directNouns.size(); i++) {
				frames.add((ITableDataFrame) directNouns.get(i).getValue());
			}
		}
		return frames;
	}

	@Override
	public List<NounMetadata> getOutputs() {
		List<NounMetadata> outputs = super.getOutputs();
		if(outputs != null) {
			return outputs;
		}

		outputs = new Vector<NounMetadata>();
		NounMetadata output = new NounMetadata(this.signature, PixelDataType.FRAME, PixelOperationType.FRAME);
		outputs.add(output);
		return outputs;
	}

}
