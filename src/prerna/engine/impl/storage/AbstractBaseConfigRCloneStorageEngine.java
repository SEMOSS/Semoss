package prerna.engine.impl.storage;

public abstract class AbstractBaseConfigRCloneStorageEngine  extends AbstractRCloneStorageEngine {

	{
		this.PROVIDER = "s3";
	}

	/**
	 * While these are not final values as they are set from the smss
	 * They should not be altered
	 */
	
	protected String BUCKET = null;
	protected String REGION = null;
	protected String ACCESS_KEY = null;
	protected String SECRET_KEY = null;

}
