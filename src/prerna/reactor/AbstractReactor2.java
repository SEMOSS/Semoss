package prerna.reactor;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;

import prerna.reactor.annotations.ReactorKey;
import prerna.reactor.annotations.ReactorOutput;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

/**
 * AbstractReactor2 - A more intuitive way to define reactors using annotations
 * 
 * Features:
 * - Use @ReactorKey annotation on fields to define input parameters
 * - Use @ReactorOutput annotation on class to define output type  
 * - Automatic parameter validation and type conversion
 * - Minimal boilerplate code - just annotate fields and implement executeReactor()
 * 
 * Example usage:
 * 
 * @ReactorOutput(description = "Returns a greeting message", dataType = PixelDataType.CONST_STRING)
 * public class GreetingReactor extends AbstractReactor2 {
 * 
 *     @ReactorKey(key = "name", description = "Person's name to greet", required = true)
 *     private String name;
 * 
 *     @ReactorKey(key = "greeting", description = "Greeting prefix", defaultValue = "Hello")
 *     private String greeting;
 * 
 *     @Override
 *     public NounMetadata executeReactor() {
 *         return new NounMetadata(greeting + ", " + name + "!", PixelDataType.CONST_STRING);
 *     }
 * }
 */
public abstract class AbstractReactor2 extends AbstractReactor {

    private static final Logger classLogger = LogManager.getLogger(AbstractReactor2.class);
    
    // Cache for reflection data to avoid repeated processing
    private static final Map<Class<?>, ReactorMetadata> REACTOR_METADATA_CACHE = new HashMap<>();
    
    // Gson instance for JSON/Map conversion
    private static final Gson GSON = new Gson();
    
    private ReactorMetadata metadata;
    private Map<String, Object> parameterValues = new HashMap<>();
    
    public AbstractReactor2() {
        super();
        initializeFromAnnotations();
    }
    
    /**
     * Initialize the reactor based on annotations
     */
    private void initializeFromAnnotations() {
        Class<?> reactorClass = this.getClass();
        
        // Check cache first
        if (REACTOR_METADATA_CACHE.containsKey(reactorClass)) {
            this.metadata = REACTOR_METADATA_CACHE.get(reactorClass);
        } else {
            this.metadata = extractMetadata(reactorClass);
            REACTOR_METADATA_CACHE.put(reactorClass, this.metadata);
        }
        
        // Set up the base class arrays
        setupBaseClassArrays();
    }
    
    /**
     * Extract metadata from annotations
     */
    private ReactorMetadata extractMetadata(Class<?> reactorClass) {
        ReactorMetadata metadata = new ReactorMetadata();
        
        // Extract output annotation
        ReactorOutput outputAnnotation = reactorClass.getAnnotation(ReactorOutput.class);
        if (outputAnnotation != null) {
            metadata.outputDescription = outputAnnotation.description();
            metadata.outputDataType = outputAnnotation.dataType();
            metadata.outputMulti = outputAnnotation.multi();
        }
        
        // Extract field annotations
        Field[] fields = reactorClass.getDeclaredFields();
        for (Field field : fields) {
            ReactorKey keyAnnotation = field.getAnnotation(ReactorKey.class);
            if (keyAnnotation != null) {
                ParameterMetadata paramMeta = new ParameterMetadata();
                paramMeta.field = field;
                paramMeta.key = keyAnnotation.key();
                paramMeta.description = keyAnnotation.description();
                paramMeta.required = keyAnnotation.required();
                paramMeta.multi = keyAnnotation.multi();
                paramMeta.dataType = keyAnnotation.dataType();
                paramMeta.defaultValue = keyAnnotation.defaultValue();
                paramMeta.encoded = keyAnnotation.encoded();
                
                // Make field accessible
                field.setAccessible(true);
                
                metadata.parameters.put(paramMeta.key, paramMeta);
            }
        }
        
        return metadata;
    }
    
    /**
     * Set up the arrays that the base AbstractReactor expects
     */
    private void setupBaseClassArrays() {
        int paramCount = metadata.parameters.size();
        
        this.keysToGet = new String[paramCount];
        this.keyRequired = new int[paramCount];
        this.keyMulti = new int[paramCount];
        this.keyDefaults = new Object[paramCount];
        
        int index = 0;
        for (ParameterMetadata param : metadata.parameters.values()) {
            this.keysToGet[index] = param.key;
            this.keyRequired[index] = param.required ? 1 : 0;
            this.keyMulti[index] = param.multi ? 1 : 0;
            
            // Set default value if provided
            if (!param.defaultValue.isEmpty()) {
                this.keyDefaults[index] = convertDefaultValue(param.defaultValue, param.dataType);
            }
            
            index++;
        }
    }
    
    /**
     * Convert string default value to appropriate type
     */
    private Object convertDefaultValue(String defaultValue, PixelDataType dataType) {
        try {
            switch (dataType) {
                case CONST_INT:
                    return Integer.parseInt(defaultValue);
                case CONST_DECIMAL:
                    return Double.parseDouble(defaultValue);
                case BOOLEAN:
                    return Boolean.parseBoolean(defaultValue);
                case MAP:
                    if (defaultValue.trim().startsWith("{")) {
                        return GSON.fromJson(defaultValue, new TypeToken<Map<String, Object>>(){}.getType());
                    } else {
                        return new HashMap<String, Object>();
                    }
                case CONST_STRING:
                default:
                    return defaultValue;
            }
        } catch (Exception e) {
            classLogger.warn("Failed to convert default value '{}' to type {}, using as string", defaultValue, dataType);
            return defaultValue;
        }
    }
    
    @Override
    public final NounMetadata execute() {
        try {
            // Organize keys from parent class
            organizeKeys();
            
            // Extract and validate parameters
            extractParameters();
            
            // Inject parameters into annotated fields
            injectParameters();
            
            // Execute the reactor logic
            return executeReactor();
            
        } catch (Exception e) {
            classLogger.error("Error executing reactor {}: {}", this.getClass().getSimpleName(), e.getMessage(), e);
            return NounMetadata.getErrorNounMessage("Error executing reactor: " + e.getMessage());
        }
    }
    
    /**
     * Extract parameters from the noun store and convert to appropriate types
     */
    private void extractParameters() throws SemossPixelException {
        for (ParameterMetadata param : metadata.parameters.values()) {
            Object value = extractParameter(param);
            if (value != null) {
                parameterValues.put(param.key, value);
            } else if (param.required) {
                throw new SemossPixelException("Required parameter '" + param.key + "' is missing");
            } else if (!param.defaultValue.isEmpty()) {
                parameterValues.put(param.key, convertDefaultValue(param.defaultValue, param.dataType));
            }
        }
    }
    
    /**
     * Extract a single parameter from the noun store
     */
    private Object extractParameter(ParameterMetadata param) throws SemossPixelException {
        GenRowStruct grs = this.store.getNoun(param.key);
        if (grs == null || grs.isEmpty()) {
            // Try getting from curRow if not found in store
            if (this.curRow != null && !this.curRow.isEmpty()) {
                // Try to find by index in keysToGet
                for (int i = 0; i < keysToGet.length; i++) {
                    if (keysToGet[i].equals(param.key) && i < this.curRow.size()) {
                        NounMetadata noun = this.curRow.getNoun(i);
                        return convertValue(noun.getValue(), param.dataType, param.multi, param.encoded);
                    }
                }
            }
            return null;
        }
        
        if (param.multi) {
            // Return list of values
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < grs.size(); i++) {
                Object convertedValue = convertValue(grs.get(i), param.dataType, false, param.encoded);
                values.add(convertedValue);
            }
            return values;
        } else {
            // Return single value
            return convertValue(grs.get(0), param.dataType, false, param.encoded);
        }
    }
    
    /**
     * Convert value to the expected type
     */
    @SuppressWarnings("unchecked")
    private Object convertValue(Object value, PixelDataType expectedType, boolean isList, boolean encoded) throws SemossPixelException {
        if (value == null) {
            return null;
        }
        
        String stringValue = value.toString();
        
        // Apply decoding if needed
        if (encoded && stringValue != null) {
            stringValue = Utility.decodeURIComponent(stringValue);
        }
        
        try {
            switch (expectedType) {
                case CONST_STRING:
                    return stringValue;
                case CONST_INT:
                    if (value instanceof Number) {
                        return ((Number) value).intValue();
                    }
                    return Integer.parseInt(stringValue);
                case CONST_DECIMAL:
                    if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                    }
                    return Double.parseDouble(stringValue);
                case BOOLEAN:
                    if (value instanceof Boolean) {
                        return value;
                    }
                    return Boolean.parseBoolean(stringValue);
                case MAP:
                    // Handle Map conversion
                    if (value instanceof Map) {
                        return value;
                    } else {
                        // Try to parse as JSON
                        try {
                            if (stringValue.trim().startsWith("{")) {
                                return GSON.fromJson(stringValue, new TypeToken<Map<String, Object>>(){}.getType());
                            } else {
                                throw new SemossPixelException("Map parameter must be a valid JSON object string starting with '{'");
                            }
                        } catch (JsonSyntaxException e) {
                            throw new SemossPixelException("Invalid JSON format for Map parameter: " + e.getMessage());
                        }
                    }
                default:
                    return encoded ? stringValue : value;
            }
        } catch (NumberFormatException e) {
            throw new SemossPixelException("Cannot convert '" + stringValue + "' to " + expectedType);
        }
    }
    
    /**
     * Inject extracted parameters into annotated fields
     */
    private void injectParameters() {
        for (ParameterMetadata param : metadata.parameters.values()) {
            Object value = parameterValues.get(param.key);
            if (value != null) {
                try {
                    param.field.set(this, value);
                } catch (IllegalAccessException e) {
                    classLogger.error("Failed to inject parameter '{}' into field", param.key, e);
                }
            }
        }
    }
    
    /**
     * Abstract method that subclasses must implement for their reactor logic
     */
    public abstract NounMetadata executeReactor() throws SemossPixelException;
    
    // Convenience methods for getting parameters with type safety
    
    /**
     * Get a string parameter value
     */
    protected String getString(String key) {
        Object value = parameterValues.get(key);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Get a string parameter value with default
     */
    protected String getString(String key, String defaultValue) {
        String value = getString(key);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Get an integer parameter value
     */
    protected Integer getInt(String key) {
        Object value = parameterValues.get(key);
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        return null;
    }
    
    /**
     * Get an integer parameter value with default
     */
    protected int getInt(String key, int defaultValue) {
        Integer value = getInt(key);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Get a double parameter value
     */
    protected Double getDouble(String key) {
        Object value = parameterValues.get(key);
        if (value instanceof Double) {
            return (Double) value;
        } else if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return null;
    }
    
    /**
     * Get a double parameter value with default
     */
    protected double getDouble(String key, double defaultValue) {
        Double value = getDouble(key);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Get a boolean parameter value
     */
    protected Boolean getBoolean(String key) {
        Object value = parameterValues.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return null;
    }
    
    /**
     * Get a boolean parameter value with default
     */
    protected boolean getBoolean(String key, boolean defaultValue) {
        Boolean value = getBoolean(key);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Get a list parameter value
     */
    @SuppressWarnings("unchecked")
    protected <T> List<T> getList(String key) {
        Object value = parameterValues.get(key);
        if (value instanceof List) {
            return (List<T>) value;
        }
        return null;
    }
    
    /**
     * Get a Map parameter value
     */
    @SuppressWarnings("unchecked")
    protected Map<String, Object> getMap(String key) {
        Object value = parameterValues.get(key);
        if (value instanceof Map) {
            return (Map<String, Object>) value;
        }
        return null;
    }
    
    /**
     * Get a Map parameter value with default
     */
    protected Map<String, Object> getMap(String key, Map<String, Object> defaultValue) {
        Map<String, Object> value = getMap(key);
        return value != null ? value : defaultValue;
    }
    
    /**
     * Get a typed Map parameter value (for stronger typing when you know the value types)
     */
    @SuppressWarnings("unchecked")
    protected <K, V> Map<K, V> getTypedMap(String key) {
        Object value = parameterValues.get(key);
        if (value instanceof Map) {
            return (Map<K, V>) value;
        }
        return null;
    }
    
    /**
     * Get a value from a Map parameter
     */
    protected Object getMapValue(String mapKey, String valueKey) {
        Map<String, Object> map = getMap(mapKey);
        return map != null ? map.get(valueKey) : null;
    }
    
    /**
     * Get a String value from a Map parameter
     */
    protected String getMapString(String mapKey, String valueKey) {
        Object value = getMapValue(mapKey, valueKey);
        return value != null ? value.toString() : null;
    }
    
    /**
     * Get a String value from a Map parameter with default
     */
    protected String getMapString(String mapKey, String valueKey, String defaultValue) {
        String value = getMapString(mapKey, valueKey);
        return value != null ? value : defaultValue;
    }
    
    @Override
    protected String getDescriptionForKey(String key) {
        ParameterMetadata param = metadata.parameters.get(key);
        if (param != null && !param.description.isEmpty()) {
            return param.description;
        }
        return super.getDescriptionForKey(key);
    }
    
    @Override
    public String getReactorDescription() {
        if (metadata != null && !metadata.outputDescription.isEmpty()) {
            return metadata.outputDescription;
        }
        return "Reactor implemented using AbstractReactor2";
    }
    
    /**
     * Metadata holder classes
     */
    private static class ReactorMetadata {
        String outputDescription = "";
        PixelDataType outputDataType = PixelDataType.CONST_STRING;
        boolean outputMulti = false;
        Map<String, ParameterMetadata> parameters = new HashMap<>();
    }
    
    private static class ParameterMetadata {
        Field field;
        String key;
        String description;
        boolean required;
        boolean multi;
        PixelDataType dataType;
        String defaultValue;
        boolean encoded;
    }
}