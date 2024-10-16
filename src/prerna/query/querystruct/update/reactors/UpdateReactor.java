package prerna.query.querystruct.update.reactors;

import java.util.List;
import java.util.Vector;

import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.query.querystruct.update.UpdateQueryStruct;
import prerna.reactor.qs.AbstractQueryStructReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class UpdateReactor extends AbstractQueryStructReactor {

	public UpdateReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.COLUMNS.getKey(), ReactorKeysEnum.VALUES.getKey()};
	}
	
	@Override
	protected UpdateQueryStruct createQueryStruct() {
		UpdateQueryStruct qs = new UpdateQueryStruct();
		// merge any existing values
		if(this.qs != null) {
			qs.merge(this.qs);
			qs.setQsType(this.qs.getQsType());
		}
		
		GenRowStruct col_grs = this.store.getNoun(this.keysToGet[0]);
		GenRowStruct val_grs = this.store.getNoun(this.keysToGet[1]);
		
		List<IQuerySelector> columns = new Vector<>();
		List<Object> values = new Vector<>();
		
		for(int i = 0; i < col_grs.size(); i++) {
			String col = col_grs.get(i) + "";
			Object val = val_grs.get(i);
			
			QueryColumnSelector colS = new QueryColumnSelector(col);
			columns.add(colS);
			if(val instanceof List) {
				if(((List) val).size() > 1) {
					NounMetadata n = new NounMetadata("Can only specify one value to update to", PixelDataType.CONST_STRING, PixelOperationType.ERROR);
					throw new SemossPixelException(n);
				}
				Object val0 = ((List) val).get(0);
				if(val0 instanceof NounMetadata) {
					values.add( ((NounMetadata) val0).getValue() );
				} else {
					values.add(val0);
				}
			} else {
				if(val instanceof NounMetadata) {
					values.add( ((NounMetadata) val).getValue() );
				} else {
					values.add(val);
				}
			}
		}
		
		qs.setSelectors(columns);
		qs.setValues(values);
		this.qs = qs;
		return qs;
	}
}
