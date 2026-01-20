/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
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