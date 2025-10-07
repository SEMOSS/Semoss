package prerna.reactor.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import prerna.sablecc2.om.PixelDataType;

/**
 * Annotation to define reactor output type and description
 * Used to document what the reactor returns
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ReactorOutput {
    
    /**
     * Description of what this reactor outputs
     */
    String description() default "";
    
    /**
     * The data type that this reactor returns
     */
    PixelDataType dataType() default PixelDataType.CONST_STRING;
    
    /**
     * Whether this reactor can return multiple values
     */
    boolean multi() default false;
    
    /**
     * Whether the output should be encoded when returned
     * Useful when returning complex data structures as strings
     */
    boolean encoded() default false;
}