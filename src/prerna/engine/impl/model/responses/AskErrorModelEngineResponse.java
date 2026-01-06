package prerna.engine.impl.model.responses;

import java.util.Map;

public class AskErrorModelEngineResponse extends AskModelEngineResponse<String> {
    
    public static final String ERROR_TYPE = "error_type";
    public static final String CODE = "code";
    public static final String CLIENT = "client";
    public static final String MODEL = "model";

    protected String errorType;
    protected int code;
    protected String client;
    protected String model;

    public AskErrorModelEngineResponse(String message, String errorType, int code, String client, String model) {
        super(message, 0, 0);
        this.messageType = "ERROR";
        this.errorType = errorType;
        this.code = code;
        this.client = client;
        this.model = model;
    }

    @Override
    public String getStringResponse() {
        return (String) this.response;
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> map = super.toMap();
        map.put(ERROR_TYPE, this.errorType);
        map.put(CODE, this.code);
        map.put(CLIENT, this.client);
        map.put(MODEL, this.model);
        return map;
    }
}