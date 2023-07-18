package prerna.engine.impl.rdbms;

import prerna.engine.api.IEngine;
import prerna.query.interpreters.IQueryInterpreter;
import prerna.query.interpreters.sql.ImpalaSqlInterpreter;

@Deprecated
public class ImpalaEngine extends RDBMSNativeEngine {


	/*
	 * Reviewed 2023-07-18
	 * We do not need this class anymore, we have moved this to the query util class in RDBMSNativeEngine
	 */
	
	/*
	 * WE ONLY HAVE THIS CLASS BECAUSE OF THE QUEYR WRAPPER
	 * WEIRD WORD LOWER CASEING HAPPENS FROM THE RESUTL SET METADATA
	 * SO WE HAVE OUR OWN ENGINE AND IT HAS ITS OWN ENGINE TYPE WITH ASSOCIATED 
	 * WRAPPER
	 * 
	 * PLEASE TRY TO USE THE DEFAULT RDBMSNativeEngine WHEN POSSIBLE
	 * INSTEAD OF MAKING A NEW CLASS
	 * 
	 */
	
	public ImpalaEngine() {
		
	}
	
	public IQueryInterpreter getQueryInterpreter(){
		return new ImpalaSqlInterpreter(this);
	}
	
	public ENGINE_TYPE getEngineType() {
		return IEngine.ENGINE_TYPE.IMPALA;
	}

}
