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
package prerna.logging;

import com.google.gson.*;
import java.lang.reflect.Type;
import java.sql.SQLException;
import java.util.*;

public class LoggingThrowableSerializer implements JsonSerializer<Throwable> {
    
    private final int maxDepth;
    private final int maxStackTraceElements;
    private final boolean includeSuppressed;
    
    public LoggingThrowableSerializer() {
        this(10, 30, true);
    }
    
    public LoggingThrowableSerializer(int maxDepth, int maxStackTraceElements, boolean includeSuppressed) {
        this.maxDepth = maxDepth;
        this.maxStackTraceElements = maxStackTraceElements;
        this.includeSuppressed = includeSuppressed;
    }
    
    @Override
    public JsonElement serialize(Throwable throwable, Type typeOfSrc, JsonSerializationContext context) {
        if (throwable == null) {
            return JsonNull.INSTANCE;
        }
        
        Set<Throwable> seen = new HashSet<>();
        return serializeThrowable(throwable, 0, seen, context);
    }
    
    private JsonElement serializeThrowable(Throwable throwable, int depth, Set<Throwable> seen, JsonSerializationContext context) {
        if (throwable == null || depth > maxDepth || seen.contains(throwable)) {
            return JsonNull.INSTANCE;
        }
        
        seen.add(throwable);
        JsonObject obj = new JsonObject();
        
        // Class name
        obj.addProperty("class", throwable.getClass().getName());
        
        // Message
        obj.addProperty("message", throwable.getMessage());
        
        // Localized message (if different)
        String localizedMessage = throwable.getLocalizedMessage();
        if (localizedMessage != null && !localizedMessage.equals(throwable.getMessage())) {
            obj.addProperty("localizedMessage", localizedMessage);
        }
        
        // Stack trace
        obj.add("stackTrace", serializeStackTrace(throwable.getStackTrace()));
        
        // Cause
        if (throwable.getCause() != null && throwable.getCause() != throwable) {
            obj.add("cause", serializeThrowable(throwable.getCause(), depth + 1, seen, context));
        }
        
        // Suppressed exceptions
        if (includeSuppressed) {
            Throwable[] suppressed = throwable.getSuppressed();
            if (suppressed != null && suppressed.length > 0) {
                JsonArray suppressedArray = new JsonArray();
                for (Throwable s : suppressed) {
                    suppressedArray.add(serializeThrowable(s, depth + 1, seen, context));
                }
                obj.add("suppressed", suppressedArray);
            }
        }
        
        // SQLException specific: nextException chain
        if (throwable instanceof SQLException sqlEx) {
            SQLException next = sqlEx.getNextException();
            if (next != null && next != throwable) {
                obj.add("nextException", serializeThrowable(next, depth + 1, seen, context));
            }
            
            // SQL state and error code
            obj.addProperty("sqlState", sqlEx.getSQLState());
            obj.addProperty("errorCode", sqlEx.getErrorCode());
        }
        
        return obj;
    }
    
    private JsonArray serializeStackTrace(StackTraceElement[] stackTrace) {
        JsonArray array = new JsonArray();
        int limit = Math.min(stackTrace.length, maxStackTraceElements);
        
        for (int i = 0; i < limit; i++) {
            StackTraceElement ste = stackTrace[i];
            JsonObject element = new JsonObject();
            element.addProperty("className", ste.getClassName());
            element.addProperty("methodName", ste.getMethodName());
            element.addProperty("fileName", ste.getFileName());
            element.addProperty("lineNumber", ste.getLineNumber());
            array.add(element);
        }
        
        if (stackTrace.length > maxStackTraceElements) {
            JsonObject truncated = new JsonObject();
            truncated.addProperty("truncated", true);
            truncated.addProperty("remaining", stackTrace.length - maxStackTraceElements);
            array.add(truncated);
        }
        
        return array;
    }
}