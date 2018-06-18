package prerna.sablecc2.reactor.qs;

import java.util.List;
import java.util.Vector;

import prerna.engine.api.IEngine;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.IQuerySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class QueryInsertReactor extends AbstractReactor {
	
	private NounMetadata qStruct = null;
	
	@Override
	public NounMetadata execute() {
		NounMetadata success = new NounMetadata(false, PixelDataType.BOOLEAN);
		if(qStruct == null) {
			qStruct = getQueryStruct();
		}
		
		StringBuilder query = new StringBuilder("INSERT INTO ");
		
		GenRowStruct col_grs = this.store.getNoun("into");
		GenRowStruct val_grs = this.store.getNoun("values");
		String table = "";
		
		List<IQuerySelector> selectors = new Vector<IQuerySelector>();
		
		for(int i = 0; i < col_grs.size(); i++){
			String s = col_grs.get(i).toString();
			if(s.contains("__")) {
				selectors.add(new QueryColumnSelector (s));
			}
			else {
				table = s;
			}
		}
		
		if(table == "") {
			// Insert table name
			QueryColumnSelector t = (QueryColumnSelector) selectors.get(0);
			query.append(t.getTable()).append(" (");
			
			// Insert columns
			for(int i = 0; i < selectors.size(); i++) {
				QueryColumnSelector c = (QueryColumnSelector) selectors.get(i);
				if(i == selectors.size() - 1) {
					query.append(c.getColumn());
				}
				else {
					query.append(c.getColumn() + ", ");
				}
			}
			query.append(") VALUES (");
		}
		
		else {
			query.append(table).append(" VALUES (");
		}
		
		// Insert values
		for(int i = 0; i < val_grs.size(); i++) {
			if(i == val_grs.size() - 1) {
				if(val_grs.get(i) instanceof String) {
					query.append("'" + val_grs.get(i) + "'");
				}
				else {
					query.append(val_grs.get(i));
				}
			}
			else {
				if(val_grs.get(i) instanceof String) {
					query.append("'" + val_grs.get(i) + "', ");
				}
				else {
					query.append(val_grs.get(i) + ", ");
				}
			}
		}
		
		query.append(")");
		
		// execute query
		SelectQueryStruct qs = (SelectQueryStruct) qStruct.getValue();
		IEngine engine = qs.retrieveQueryStructEngine();
		if(engine instanceof RDBMSNativeEngine){
			engine.insertData(query.toString());
			success = new NounMetadata(true, PixelDataType.BOOLEAN);
		}
		
		System.out.println("SQL QUERY..." + query.toString());
		
		return success;
	}
	
	private NounMetadata getQueryStruct() {
		GenRowStruct allNouns = getNounStore().getNoun(PixelDataType.QUERY_STRUCT.toString());
		NounMetadata queryStruct = null;
		if(allNouns != null) {
			NounMetadata object = (NounMetadata)allNouns.getNoun(0);
			return object;
		} 
		return queryStruct;
	}
	
	public void setQueryStruct(NounMetadata qs) {
		this.qStruct = qs;
	}
	
	public void setNounStore(NounStore ns) {
		this.store = ns;
	}
}
