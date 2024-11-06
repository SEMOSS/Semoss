package prerna.reactor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/** ExecuteAppAsAPI ( appId = '10528cfb-a503-4cf1-aff7-7880bf014e12' , map = [ { } ] ) ;
 * @author ritdoshi
 *
 */

public class ExecuteAppAsAPI extends AbstractReactor {
	
	public static final String projectDir = "C:\\workspace\\Semoss\\project";
	
	public ExecuteAppAsAPI() {
		this.keysToGet = new String[] {ReactorKeysEnum.APP_ID.getKey(), ReactorKeysEnum.MAP.getKey()};
		this.keyRequired = new int[] {1, 1};
	}

	@Override
	public NounMetadata execute() {
		
		organizeKeys();
		
		String appId = this.keyValue.get(ReactorKeysEnum.APP_ID.getKey());
//		Object inputMap = this.keyValue.get(ReactorKeysEnum.MAP.getKey()); // workaround for now until input mapper works
		
		getAppDir(appId);

		return new NounMetadata(appId, PixelDataType.CONST_STRING);
	}

	private void getAppDir(String appId) {
		try {
			Stream<Path> paths = Files.list(Paths.get(projectDir));
			List<Path> folders = paths.filter(Files::isDirectory).filter(path -> path.getFileName().toString().contains(appId)).collect(Collectors.toList());
			assert folders.size() == 1;
			Path targetPath = folders.get(0);
			Stream<Path> files = Files.walk(targetPath);
			files.forEach(System.out::println);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			throw new SemossPixelException("File operation failed", e);
		} catch (AssertionError e) {
			throw new SemossPixelException("Assert failed", e);
		}
	}
	
//	  public static <T> T getPayloadObject(
//		      NounStore nounStore, String[] keysToGet, Class<T> targetClass) {
//		    Map<String, Object> payloadMap = new HashMap<>();
//		    for (String key : keysToGet) {
//		      if ("no keys defined".equals(key)) {
//		        break;
//		      }
//		      GenRowStruct grs = nounStore.getNoun(key);
//		      if (grs == null || grs.isEmpty()) {
//		        payloadMap.put(key, null);
//		      } else {
//		        payloadMap.put(key, grs.getAllValues());
//		      }
//		    }
//		    T payload;
//		    try {
////		      payload = CustomMapper.PAYLOAD_MAPPER.convertValue(payloadMap, targetClass);
////		    	Need to find a mapper 
//		    } catch (IllegalArgumentException e) {
//
//		    return payload;
//		  }

}
