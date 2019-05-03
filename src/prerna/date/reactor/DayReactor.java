package prerna.date.reactor;

import prerna.date.SemossDay;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class DayReactor extends AbstractReactor {
	
	public DayReactor() {
		this.keysToGet = new String[]{"days"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String numDays = this.keyValue.get(this.keysToGet[0]);
		SemossDay day = new SemossDay(numDays);
		return new NounMetadata(day, PixelDataType.CONST_DAY);
	}
}
