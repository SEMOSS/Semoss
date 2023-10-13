package prerna.reactor.frame.convert;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.GenRowFilters;
import prerna.reactor.frame.AbstractFrameReactor;
import prerna.reactor.frame.FrameFactory;
import prerna.reactor.imports.FrameSizeRetrictions;
import prerna.reactor.imports.IImporter;
import prerna.reactor.imports.ImportFactory;
import prerna.reactor.imports.ImportUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.VarStore;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;

public class ConvertReactor extends AbstractFrameReactor {

	private static final Logger classLogger = LogManager.getLogger(ConvertReactor.class);
	
	public ConvertReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME_TYPE.getKey(), ReactorKeysEnum.FRAME.getKey(), 
				ReactorKeysEnum.ALIAS.toString()};
	}
	
	@Override
	public NounMetadata execute() {
		ITableDataFrame frame = getFrame();
		GenRowFilters curFilters = frame.getFrameFilters().copy();
		SelectQueryStruct qs = frame.getMetaData().getFlatTableQs(false);
		qs.setFrame(frame);
		if(qs.getSelectors().size() == 0) {
			throw new IllegalArgumentException("There are no selectors in this frame to move to R");
		}
		
		IRawSelectWrapper it = null;
		ITableDataFrame newFrame = null;
		// get the name of the frame type
		String frameType = getFrameType();
		String alias = getAlias();
		try {
			if(!(frame instanceof NativeFrame)) {
				try {
					it = frame.query(qs);
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new SemossPixelException(
							new NounMetadata("Error occurred executing query before loading into frame", 
									PixelDataType.CONST_STRING, PixelOperationType.ERROR));
				}
				try {
					if(!FrameSizeRetrictions.importWithinLimit(frame, it)) {
						SemossPixelException exception = new SemossPixelException(
								new NounMetadata("Frame size is too large, please limit the data size before proceeding", 
										PixelDataType.CONST_STRING, 
										PixelOperationType.FRAME_SIZE_LIMIT_EXCEEDED, PixelOperationType.ERROR));
						exception.setContinueThreadOfExecution(false);
						throw exception;
					}
				} catch (SemossPixelException e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw e;
				} catch (Exception e) {
					classLogger.error(Constants.STACKTRACE, e);
					throw new SemossPixelException(getError("Error occurred executing query before loading into frame"));
				}
			}
			
			// will assume the person wants me to 
			// override the existing variable
			if(alias == null) {
				alias = frame.getName();
			}
			try {
				newFrame = FrameFactory.getFrame(this.insight, frameType, alias);
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new IllegalArgumentException("Error occurred trying to create frame of type " + frameType, e);
			}
			// insert the data for the new frame
			IImporter importer = ImportFactory.getImporter(newFrame, qs, it);
			try {
				importer.insertData();
			} catch (Exception e) {
				classLogger.error(Constants.STACKTRACE, e);
				throw new SemossPixelException(e.getMessage());
			}
		} finally {
			if(it != null) {
				try {
					it.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		// merge existing metadata
		ImportUtility.mergeFlatTableSources(newFrame.getMetaData(), frame.getMetaData().getHeaderToSources());

		NounMetadata noun = new NounMetadata(newFrame, PixelDataType.FRAME, PixelOperationType.FRAME);
		// see if this is overriding any reference
		VarStore varStore = this.insight.getVarStore();
		// if it is the same name
		// override the reference
		if(frame.getName().equals(alias)) {
			newFrame.setFrameFilters(curFilters);
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
		GenRowStruct grs = this.store.getNoun(PixelDataType.ALIAS.getKey());
		// see if a frame is passed in
		if (grs != null && !grs.isEmpty()) {
			String alias = grs.getNoun(0).getValue()+"";
			return alias;
		}
		
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
		GenRowStruct grs = this.store.getNoun(this.keysToGet[0]);
		if(grs != null && !grs.isEmpty()) {
			return grs.get(0).toString();
		}
		
		List<String> inputValues = this.curRow.getAllStrValues();
		if(!inputValues.isEmpty()) {
			return inputValues.get(0);
		}
		throw new IllegalArgumentException("Must define the output frame type");
	}
	
}
