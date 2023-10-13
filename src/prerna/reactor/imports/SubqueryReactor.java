package prerna.reactor.imports;

import prerna.algorithm.api.ITableDataFrame;
import prerna.ds.nativeframe.NativeFrame;
import prerna.om.Insight;
import prerna.query.querystruct.AbstractQueryStruct;
import prerna.query.querystruct.AbstractQueryStruct.QUERY_STRUCT_TYPE;
import prerna.query.querystruct.SQLQueryUtils;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.transform.QSAliasToPhysicalConverter;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class SubqueryReactor extends AbstractReactor {
	
	public SubqueryReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.FRAME.getKey(), ReactorKeysEnum.QUERY_STRUCT.getKey()};
	}

	@Override
	public NounMetadata execute()  {
		ITableDataFrame curFrame = this.insight.getCurFrame();
		SelectQueryStruct qs = getQueryStruct();
		if(qs != null) {
			AbstractQueryStruct.QUERY_STRUCT_TYPE type = qs.getQsType();
			if( (type == QUERY_STRUCT_TYPE.FRAME || type == QUERY_STRUCT_TYPE.RAW_FRAME_QUERY) && qs.getFrame() == null) {
				qs.setFrame(curFrame);
			}
		}
		ITableDataFrame mergeFrame = null;
		
		// are they both native
		if(curFrame instanceof NativeFrame && curFrame != null && qs !=null)
		{
			try
			{
				qs = QSAliasToPhysicalConverter.getPhysicalQs(qs, qs.getFrame().getMetaData());
				mergeFrame = SQLQueryUtils.subQuery(((NativeFrame)curFrame).getQueryStruct(), qs);
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
		
		// clear cached info after merge
		curFrame.clearCachedMetrics();
		curFrame.clearQueryCache();

		NounMetadata noun = new NounMetadata(mergeFrame, PixelDataType.FRAME, PixelOperationType.FRAME_DATA_CHANGE, PixelOperationType.FRAME_HEADERS_CHANGE);
		// in case we generated a new frame
		// update existing references
		if(mergeFrame != curFrame) {
			if(curFrame.getName() != null) {
				this.insight.getVarStore().put(curFrame.getName(), noun);
			} 
			if(curFrame == this.insight.getVarStore().get(Insight.CUR_FRAME_KEY).getValue()) {
				this.insight.setDataMaker(mergeFrame);
			}
		}
		
		return noun;
	}
	
	
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////
	
	
	protected SelectQueryStruct getQueryStruct() {
		SelectQueryStruct queryStruct = null;

		GenRowStruct grs = this.store.getNoun(this.keysToGet[1]);
		if(grs != null) {
			NounMetadata object = (NounMetadata)grs.getNoun(0);
			return (SelectQueryStruct)object.getValue();
		}
		
		grs = this.store.getNoun(PixelDataType.QUERY_STRUCT.toString());
		if(grs != null) {
			NounMetadata object = (NounMetadata) grs.getNoun(0);
			return (SelectQueryStruct)object.getValue();
		}

		return queryStruct;
	}

}