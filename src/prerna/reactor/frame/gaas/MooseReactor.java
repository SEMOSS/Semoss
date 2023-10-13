package prerna.reactor.frame.gaas;

import java.util.HashMap;
import java.util.Map;

import prerna.reactor.AbstractReactor;
import prerna.reactor.frame.gaas.chat.MooseChatReactor;
import prerna.reactor.frame.gaas.ner.FillFormReactor;
import prerna.reactor.frame.gaas.qa.QueryQAModelReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

public class MooseReactor extends GaasBaseReactor {

	// we could move this to RDF Map also later
	Map <String, Class> commandReactorMap = new HashMap<String, Class>(); 
	
	public MooseReactor()
	{
		this.keysToGet = new String[] {ReactorKeysEnum.COMMAND.getKey(), ReactorKeysEnum.PROJECT.getKey()};
		this.keyRequired = new int[] {1, 0};
		
		commandReactorMap.put("text2sql", NLPQuery2Reactor.class);
		commandReactorMap.put("docqa", QueryQAModelReactor.class);
		commandReactorMap.put("chat", MooseChatReactor.class);
		commandReactorMap.put("lfqa", QueryQAModelReactor.class);
		commandReactorMap.put("fillform", FillFormReactor.class);
		commandReactorMap.put("text2viz", NLPQuery2Reactor.class); // need to replace this
	}
	
	@Override
	public NounMetadata execute() 
	{
		// TODO Auto-generated method stub
		// some key things
		
		// command
		// project_id
		// other data - optional
		String command = this.store.getNoun(keysToGet[0]).get(0).toString();
		String realCommand = command.substring(0, command.indexOf(":")).toLowerCase();
		String newCommand = command.substring(command.indexOf(":") + 1);
		
		
		if(commandReactorMap.containsKey(realCommand))
		{
			try {
				AbstractReactor reactor = (AbstractReactor)commandReactorMap.get(realCommand).newInstance();
				String projectId = getProjectId();
				if(projectId == null && realCommand.equalsIgnoreCase("docqa"))
				{
					// swap the reactor
					// this is a quick fix
					reactor = new MooseChatReactor();
				}
				GenRowStruct commandStruct = new GenRowStruct();
				commandStruct.addLiteral(newCommand);
				this.store.removeNoun(keysToGet[0]);
				this.store.addNoun(keysToGet[0], commandStruct);

				reactor.setNounStore(this.store);
				reactor.setInsight(insight);
				return reactor.execute();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return null;
	}
	
	
	

}
