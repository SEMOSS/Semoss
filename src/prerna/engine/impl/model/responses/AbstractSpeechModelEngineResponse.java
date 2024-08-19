package prerna.engine.impl.model.responses;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class AbstractSpeechModelEngineResponse<T> implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	public static final String FILE_PATH = "file_path";
	
	private String file_path;
	
	public AbstractSpeechModelEngineResponse(
			String file_path
			) {
		this.file_path = file_path;
	}
	
    public String getFilePath() {
        return file_path;
    }

    public void setFilePath(String file_path) {
        this.file_path = file_path;
    }
    
    public Map<String, Object> toMap() {
    	Map<String, Object> responseMap = new HashMap<>();
    	responseMap.put(FILE_PATH, this.file_path);
    	
    	return responseMap;
    }
    
    protected static Integer getInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Double) {
            return ((Double) value).intValue();
        } else if (value instanceof String) {
            return Integer.valueOf((String) value);
        } else {
            return null;
        }
    }

}
