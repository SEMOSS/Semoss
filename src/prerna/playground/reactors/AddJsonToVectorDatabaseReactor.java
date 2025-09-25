package prerna.playground.reactors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.AssetUtility;
import prerna.util.Utility;

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
            ReactorKeysEnum.VECTORDB.getKey(),
            ReactorKeysEnum.FILE_PATH.getKey(),
            ReactorKeysEnum.SPACE.getKey(), 
            "jsonFields",
            ReactorKeysEnum.PARAM_VALUES_MAP.getKey() // 3, not sure what this is for
        };

        this.keyRequired = new int[]{1, 1, 1, 0};
    }
	
	
	@Override
	public NounMetadata execute() {
		
		String vectorDatabaseId = this.keyValue.get(ReactorKeysEnum.VECTORDB.getKey());
		String space = this.keyValue.get(ReactorKeysEnum.SPACE.getKey());
		String rootFolder =  AssetUtility.getRootFolderPath(this.insight, space, false);
		
		String filePath = rootFolder + this.keyValue.get(ReactorKeysEnum.FILE_PATH.getKey());
		filePath = Utility.normalizePath(filePath);

		//We now have the file path. we need to access these files!
		
		//grab the json fields, need to access nounStore directly (store)
		
		
		
		//grab it
		//pull out contents and all that
		
		
		
		return new NounMetadata(filePath, PixelDataType.CONST_STRING);
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
