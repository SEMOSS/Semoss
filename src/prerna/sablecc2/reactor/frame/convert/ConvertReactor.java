package prerna.sablecc2.reactor.frame.convert;

import java.util.List;
import java.util.Set;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.frame.AbstractFrameReactor;
import prerna.sablecc2.reactor.frame.FrameFactory;
import prerna.sablecc2.reactor.imports.IImporter;
import prerna.sablecc2.reactor.imports.ImportFactory;
import prerna.sablecc2.reactor.imports.ImportSizeRetrictions;

public class ConvertReactor extends AbstractFrameReactor {

	public ConvertReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.FRAME_TYPE.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		SelectQueryStruct qs = frame.getMetaData().getFlatTableQs();
		if(qs.getSelectors().size() == 0) {
			throw new IllegalArgumentException("There are no selectors in this frame to move to R");
		}
		
		IRawSelectWrapper it = null;
		if(!(frame instanceof NativeFrame)) {
			try {
				it = frame.query(qs);
			} catch (Exception e) {
				e.printStackTrace();
				throw new SemossPixelException(
						new NounMetadata("Error occured executing query before loading into frame", 
								PixelDataType.CONST_STRING, PixelOperationType.ERROR));
			}
			if(!ImportSizeRetrictions.importWithinLimit(frame, it)) {
				SemossPixelException exception = new SemossPixelException(
						new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
								PixelDataType.CONST_STRING, 
								PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
				exception.setContinueThreadOfExecution(false);
				throw exception;
			}
		}
		
		// get the name of the frame type
		String frameType = getFrameType();
		String alias = getAlias();
		ITableDataFrame newFrame = FrameFactory.getFrame(this.insight, frameType, alias);
		// insert the data for the new frame
		IImporter importer = ImportFactory.getImporter(newFrame, qs, it);
		importer.insertData();
		
		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
		// see if this is overriding any reference
		VarStore varStore = this.insight.getVarStore();
		// if it is the same name
		// override the reference
		if(frame.getName().equals(alias)) {
			Set<String> curReferences = varStore.getAllAliasForObjectReference(frame);
			// switch to the new frame
			for(String reference : curReferences) {
				varStore.put(reference, noun);
			}
		}
		this.insight.getVarStore().put(alias, noun);
		
		// return the noun
		return noun;
	}
	
	/**
	 * Get an alias for the frame
	 * @return
	 */
	private String getAlias() {
		List<Object> alias = this.curRow.getValuesOfType(PixelDataType.ALIAS);
		if(alias != null && alias.size() > 0) {
			return alias.get(0).toString();
		}
		return null;
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
		
		return this.curRow.getAllStrValues().get(0);
	}
	
}
