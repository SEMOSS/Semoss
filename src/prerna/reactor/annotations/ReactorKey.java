package prerna.reactor.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import prerna.sablecc2.om.PixelDataType;

/**
 * Annotation to define a reactor input key with metadata
 * Used to automatically configure reactor parameters with minimal boilerplate
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface ReactorKey {
    
    /**
     * The key name for this parameter
     */
    String key();
    
    /**
     * Human-readable description of this parameter
     */
    String description() default "";
    
    /**
     * Whether this parameter is required
     */
    boolean required() default false;
    
    /**
     * Whether this parameter accepts multiple values
     */
    boolean multi() default false;
    
    /**
     * Expected data type for this parameter
     */
    PixelDataType dataType() default PixelDataType.CONST_STRING;
    
    /**
     * Default value as a string (will be converted to appropriate type)
     */
    String defaultValue() default "";
    
    /**
     * Whether this parameter should be decoded from URI encoding when received
     * This is useful for complex parameters like JSON strings or maps that are encoded
     */
    boolean encoded() default false;
}