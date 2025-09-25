package prerna.playground.reactors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * This is a temporary reactor. It's functionality can be
 * integrated elsewhere in a more cohesive manner.
 */
public class AddJsonToVectorDatabaseReactor extends AbstractReactor{

    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();

    public AddJsonToVectorDatabaseReactor() {
        this.keysToGet = new String[]{
            "jsonFile",      // 0, required
            ReactorKeysEnum.VECTORDB.getKey(),    // 1, optional (can be null)
            ReactorKeysEnum.ROOM_ID.getKey(),     // 2, optional (not required, will use insight)
            ReactorKeysEnum.COMMAND.getKey(),     // 3, required (actual user query)
            ReactorKeysEnum.CONTEXT.getKey(),     // 4, tbd on how it is used
            ReactorKeysEnum.IMAGE.getKey(),       // 5, optional, TODO: add in support
            ReactorKeysEnum.URL.getKey(),         // 6, optional, TODO: add in support
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 7, optional
        };

        this.keyRequired = new int[]{1, 0, 0, 1, 0, 0, 0, 0};
    }
	
	
	@Override
	public NounMetadata execute() {
		// TODO Auto-generated method stub
		
		
		
		return null;
	}
	/**
	 * TODO:
	 * This class will take in a file that is a json object
	 * the fields or field to extract
	 * and an vector db engine id
	 * 
	 * It will take each selected field and combine in some standardized manner
	 * and add each to a row in a csv file we generate
	 * 
	 * this csv file + a couple other things will be used as input to
	 * the vectorEngine method which will turn these rows into embeddings
	 * will chunk each one
	 */
}
