package prerna.date.reactor;

import prerna.date.SemossWeek;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class WeekReactor extends AbstractReactor {
	
	public WeekReactor() {
		this.keysToGet = new String[]{"weeks"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String numWeeks = this.keyValue.get(this.keysToGet[0]);
		SemossWeek week = new SemossWeek(numWeeks);
		return new NounMetadata(week, PixelDataType.CONST_WEEK);
	}
}
