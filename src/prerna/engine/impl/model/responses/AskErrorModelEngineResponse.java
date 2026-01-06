package prerna.engine.impl.model.responses;

import java.util.Map;

public class AskErrorModelEngineResponse extends AskModelEngineResponse<String> {
    
    public static final String ERROR_TYPE = "error_type";
    public static final String CODE = "code";
    public static final String CLIENT = "client";
    public static final String MODEL = "model";
    public static final String TRACEBACK = "traceback";

    protected String errorType;
    protected int code;
    protected String client;
    protected String model;
    protected String traceback;

    public AskErrorModelEngineResponse(String message, String errorType, int code, String client, String model, String traceback) {
        super(message, 0, 0);
        this.messageType = "ERROR";
        this.errorType = errorType;
        this.code = code;
        this.client = client;
        this.model = model;
        this.traceback = traceback;
    }

    @Override
    public String getStringResponse() {
        return (String) this.response;
    }
    
    public String getClient() { return this.client; }
    
    public String getModel() { return this.model; }
    
    public int getCode() { return this.code; }
    
    public String getTraceback() { return this.traceback; }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = super.toMap();
        map.put(ERROR_TYPE, this.errorType);
        map.put(CODE, this.code);
        map.put(CLIENT, this.client);
        map.put(MODEL, this.model);
        map.put(TRACEBACK, this.traceback);
        return map;
    }
}