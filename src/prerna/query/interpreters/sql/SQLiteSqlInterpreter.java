package prerna.query.interpreters.sql;

import prerna.algorithm.api.ITableDataFrame;
import prerna.algorithm.api.SemossDataType;
import prerna.date.SemossDate;
import prerna.engine.api.IDatabaseEngine;

public class SQLiteSqlInterpreter extends H2SqlInterpreter {

	public SQLiteSqlInterpreter() {
		
	}

	public SQLiteSqlInterpreter(IDatabaseEngine engine) {
		super(engine);
	}
	
	public SQLiteSqlInterpreter(ITableDataFrame frame) {
		super(frame);
	}
	
	@Override
	protected String formatDate(Object o, SemossDataType dateType) {
		if(o instanceof SemossDate) {
			return String.valueOf( ((SemossDate) o).getZonedDateTime().toInstant().toEpochMilli());
		} else {
			SemossDate value = SemossDate.genDateObj(o + "");
			if(value != null) {
				return String.valueOf(value.getZonedDateTime().toInstant().toEpochMilli());
			}
			
//			if(dateType == SemossDataType.DATE) {
//				SemossDate value = SemossDate.genDateObj(o + "");
//				if(value != null) {
//					return String.valueOf(value.getDate().getTime());
//				}
//			} else {
//				SemossDate value = SemossDate.genTimeStampDateObj(o + "");
//				if(value != null) {
//					return String.valueOf(value.getDate().getTime());
//				}
//			}
		}
		return o + "";
	}
}
