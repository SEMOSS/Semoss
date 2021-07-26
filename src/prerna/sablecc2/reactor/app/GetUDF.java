package prerna.sablecc2.reactor.app;

import prerna.engine.api.IEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Utility;

public class GetUDF extends AbstractReactor {
	
	private static final String CLASS_NAME = GetUDF.class.getName();
	
	// takes in a the name and engine and mounts the database assets as that variable name in both python and R
	// I need to accomodate for when I should over ride
	// for instance a user could have saved a recipe with some mapping and then later, they would like to use a different mapping

	public GetUDF()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.DATABASE.getKey()};
		this.keyRequired = new int[]{1};
	}
	
	
	@Override
	public NounMetadata execute() 
	{
		
		organizeKeys();
		
		String databaseName = keyValue.get(keysToGet[0]);
		String databaseId = Utility.getEngineData(databaseName);
		
		IEngine database = Utility.getEngine(databaseId);

		if(database != null)
		{
			String [] output = database.getUDF();
			if(output != null)
				return new NounMetadata(output, PixelDataType.VECTOR, PixelOperationType.OPERATION);
			else
				return getError("Database " + database + " - Does not have any user defined functions ");
		}
		else
			return getError("No database " + databaseName + " - Please check your spelling / case");
		
	}

}
