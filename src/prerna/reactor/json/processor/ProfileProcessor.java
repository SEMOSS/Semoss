package prerna.reactor.json.processor;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class ProfileProcessor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		System.out.println("Process a profile");
		List<String> sqlQueries = new Vector<String>();
		if(this.curRow != null && !this.curRow.isEmpty()) {
			for(int i = 0; i < this.curRow.size(); i++) {
				NounMetadata val = this.curRow.getNoun(i);
				if(val.getNounType() == PixelDataType.CONST_STRING) {
					sqlQueries.add(val.getValue().toString());
				} else if(val.getNounType() == PixelDataType.VECTOR) {
					sqlQueries.addAll( (List) val.getValue());
				}
			}
		}
		
		Hashtable<String, Object> data = this.store.getDataHash();
//		System.out.println(data);
		
		StringBuilder sb = new StringBuilder();
		sb.append("PROFILE INSERT!!!");
		sqlQueries.add(sb.toString());
		
		return new NounMetadata(sqlQueries, PixelDataType.VECTOR);
	}

}
