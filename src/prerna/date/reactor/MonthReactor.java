package prerna.date.reactor;

import prerna.date.SemossMonth;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class MonthReactor extends AbstractReactor {
	
	public MonthReactor() {
		this.keysToGet = new String[]{"months"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String numMonths = this.keyValue.get(this.keysToGet[0]);
		SemossMonth month = new SemossMonth(numMonths);
		return new NounMetadata(month, PixelDataType.CONST_MONTH);
	}
}
