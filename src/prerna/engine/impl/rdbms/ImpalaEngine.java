package prerna.engine.impl.rdbms;

import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.ImpalaInterpreter;

public class ImpalaEngine extends RDBMSNativeEngine {

	public ImpalaEngine() {
		
	}
	
	public IQueryInterpreter getQueryInterpreter2(){
		return new ImpalaInterpreter(this);
	}
	
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.IMPALA;
	}

}
