package prerna.sablecc2.reactor.json.processor;

import java.util.Hashtable;
import java.util.List;
import java.util.Vector;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class TaxIdsProcessor extends AbstractReactor {

	@Override
	public NounMetadata execute() {
		List<String> sqlQueries = new Vector<String>();
		System.out.println("Process a tax id");
		
		Hashtable<String, Object> data = this.store.getDataHash();
//		System.out.println(data);
		
		StringBuilder sb = new StringBuilder();
		sb.append("TAX INSERT QUERY!!!");
		sqlQueries.add(sb.toString());
		
		return new NounMetadata(sqlQueries, PixelDataType.VECTOR);
	}
}