package prerna.sablecc2.reactor.runtime;



import org.apache.logging.log4j.LogManager;

import prerna.sablecc2.om.nounmeta.NounMetadata;

public class BaseRUtil extends AbstractBaseRClass {

	// this is just a standard base class to remove abstract and allow other operations
	// this is really not a reactor
	
	public BaseRUtil()
	{
		this.logger = LogManager.getLogger(getClass());
	}
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		return null;
	}
	
	

}
