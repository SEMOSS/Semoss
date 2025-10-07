package prerna.reactor.examples;

import java.util.List;

import prerna.reactor.AbstractReactor2;
import prerna.reactor.annotations.ReactorKey;
import prerna.reactor.annotations.ReactorOutput;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Example reactor demonstrating the new AbstractReactor2 annotation-based approach
 * 
 * This reactor creates a personalized greeting with customizable options.
 * Compare this minimal implementation to what would be required with AbstractReactor:
 * - No need to manually define keysToGet array
 * - No need to manually define keyRequired array  
 * - No need to manually extract values from noun store
 * - Automatic type conversion and validation
 * - Built-in parameter descriptions for help
 */
@ReactorOutput(
    description = "Creates a personalized greeting message with optional formatting",
    dataType = PixelDataType.CONST_STRING
)
public class GreetingReactor extends AbstractReactor2 {

    @ReactorKey(
        key = "name", 
        description = "The person's name to include in the greeting", 
        required = true,
        dataType = PixelDataType.CONST_STRING
    )
    private String name;

    @ReactorKey(
        key = "greeting", 
        description = "The greeting prefix to use", 
        defaultValue = "Hello",
        dataType = PixelDataType.CONST_STRING
    )
    private String greeting;

    @ReactorKey(
        key = "exclamations", 
        description = "Number of exclamation marks to add", 
        defaultValue = "1",
        dataType = PixelDataType.CONST_INT
    )
    private Integer exclamations;

    @ReactorKey(
        key = "uppercase", 
        description = "Whether to convert the greeting to uppercase", 
        defaultValue = "false",
        dataType = PixelDataType.BOOLEAN
    )
    private Boolean uppercase;

    @ReactorKey(
        key = "titles", 
        description = "Optional titles to include before the name", 
        multi = true,
        dataType = PixelDataType.CONST_STRING
    )
    private List<String> titles;

    @Override
    public NounMetadata executeReactor() throws SemossPixelException {
        // Build the greeting message
        StringBuilder message = new StringBuilder();
        
        // Add greeting
        message.append(greeting);
        
        // Add titles if provided
        if (titles != null && !titles.isEmpty()) {
            message.append(" ");
            for (int i = 0; i < titles.size(); i++) {
                if (i > 0) {
                    message.append(" ");
                }
                message.append(titles.get(i));
            }
        }
        
        // Add name
        message.append(" ").append(name);
        
        // Add exclamation marks
        for (int i = 0; i < exclamations; i++) {
            message.append("!");
        }
        
        // Apply uppercase if requested
        String result = uppercase ? message.toString().toUpperCase() : message.toString();
        
        return new NounMetadata(result, PixelDataType.CONST_STRING);
    }
}

/**
 * Example usage in SEMOSS:
 * 
 * Basic greeting:
 * GreetingReactor(name=["John"]);
 * Returns: "Hello John!"
 * 
 * Custom greeting with titles:
 * GreetingReactor(name=["Smith"], greeting=["Good morning"], titles=["Dr.", "Professor"], exclamations=[3], uppercase=[true]);
 * Returns: "GOOD MORNING DR. PROFESSOR SMITH!!!"
 * 
 * Multiple names could be handled by making name multi=true and adjusting the logic
 */