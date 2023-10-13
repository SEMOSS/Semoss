package prerna.date.reactor;

import prerna.date.SemossYear;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class YearReactor extends AbstractReactor {
	
	public YearReactor() {
		this.keysToGet = new String[]{"years"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String numYears = this.keyValue.get(this.keysToGet[0]);
		SemossYear year = new SemossYear(numYears);
		return new NounMetadata(year, PixelDataType.CONST_YEAR);
	}
}
