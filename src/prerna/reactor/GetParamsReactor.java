package prerna.reactor;

import java.util.Map;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;


public class GetParamsReactor extends AbstractReactor
{

	public GetParamsReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.REACTOR.getKey()};
	}
	
	// execute method - GREEDY translation
	public NounMetadata execute()
	{
		organizeKeys();
		// get the reactor name
		// instantiate the reactor and make the call to get Params
		IReactor reactor = this.insight.getReactor(keyValue.get(keysToGet[0]));
		if(reactor == null) // try reactor factory
			reactor = ReactorFactory.getReactor(keyValue.get(keysToGet[0]), "random", null, null);
		Map <String, Map<String, String>> retMap = null;
		if(reactor instanceof AbstractReactor)
		{
			retMap = ((AbstractReactor)reactor).getReactorParams();
		}
		
		return new NounMetadata(retMap, PixelDataType.MAP, PixelOperationType.OPERATION);
	}
		
}