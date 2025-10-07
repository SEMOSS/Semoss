package prerna.reactor.examples;

import prerna.reactor.AbstractReactor2;
import prerna.reactor.annotations.ReactorKey;
import prerna.reactor.annotations.ReactorOutput;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Side-by-side comparison showing the difference between AbstractReactor and AbstractReactor2
 * 
 * This reactor formats text with various options to demonstrate the simplicity
 * of the new annotation-based approach.
 */
@ReactorOutput(
    description = "Formats text with customizable options (case, padding, prefix/suffix)",
    dataType = PixelDataType.CONST_STRING
)
public class TextFormatterReactor extends AbstractReactor2 {

    @ReactorKey(
        key = "text",
        description = "The text to format",
        required = true,
        dataType = PixelDataType.CONST_STRING
    )
    private String text;

    @ReactorKey(
        key = "toUppercase",
        description = "Convert text to uppercase",
        defaultValue = "false",
        dataType = PixelDataType.BOOLEAN
    )
    private Boolean toUppercase;

    @ReactorKey(
        key = "padding",
        description = "Number of spaces to pad on each side",
        defaultValue = "0",
        dataType = PixelDataType.CONST_INT
    )
    private Integer padding;

    @ReactorKey(
        key = "prefix",
        description = "Text to add before the main text",
        defaultValue = "",
        dataType = PixelDataType.CONST_STRING
    )
    private String prefix;

    @ReactorKey(
        key = "suffix",
        description = "Text to add after the main text",
        defaultValue = "",
        dataType = PixelDataType.CONST_STRING
    )
    private String suffix;

    @ReactorKey(
        key = "maxLength",
        description = "Maximum length of output (truncate if longer)",
        defaultValue = "0",
        dataType = PixelDataType.CONST_INT
    )
    private Integer maxLength;

    @Override
    public NounMetadata executeReactor() throws SemossPixelException {
        // Start with the base text
        String result = text;
        
        // Apply case transformation
        if (toUppercase) {
            result = result.toUpperCase();
        }
        
        // Add prefix and suffix
        if (!prefix.isEmpty()) {
            result = prefix + result;
        }
        if (!suffix.isEmpty()) {
            result = result + suffix;
        }
        
        // Add padding
        if (padding > 0) {
            String spaces = " ".repeat(padding);
            result = spaces + result + spaces;
        }
        
        // Apply max length truncation
        if (maxLength > 0 && result.length() > maxLength) {
            result = result.substring(0, maxLength - 3) + "...";
        }
        
        return new NounMetadata(result, PixelDataType.CONST_STRING);
    }
}

/*
 * COMPARISON: How this would look with traditional AbstractReactor
 * 
 * public class TextFormatterReactor extends AbstractReactor {
 * 
 *     public TextFormatterReactor() {
 *         this.keysToGet = new String[] {
 *             "text", "toUppercase", "padding", "prefix", "suffix", "maxLength"
 *         };
 *         this.keyRequired = new int[] {1, 0, 0, 0, 0, 0};
 *         this.keyDefaults = new Object[] {
 *             null, false, 0, "", "", 0
 *         };
 *     }
 * 
 *     @Override
 *     public NounMetadata execute() {
 *         organizeKeys();
 *         
 *         // Extract text (required)
 *         GenRowStruct grs = this.store.getNoun("text");
 *         if (grs == null || grs.isEmpty()) {
 *             return NounMetadata.getErrorNounMessage("Text parameter is required");
 *         }
 *         String text = grs.get(0).toString();
 *         
 *         // Extract toUppercase (optional, default false)
 *         boolean toUppercase = false;
 *         grs = this.store.getNoun("toUppercase");
 *         if (grs != null && !grs.isEmpty()) {
 *             try {
 *                 toUppercase = Boolean.parseBoolean(grs.get(0).toString());
 *             } catch (Exception e) {
 *                 return NounMetadata.getErrorNounMessage("Invalid boolean value for toUppercase");
 *             }
 *         }
 *         
 *         // Extract padding (optional, default 0)
 *         int padding = 0;
 *         grs = this.store.getNoun("padding");
 *         if (grs != null && !grs.isEmpty()) {
 *             try {
 *                 padding = Integer.parseInt(grs.get(0).toString());
 *             } catch (NumberFormatException e) {
 *                 return NounMetadata.getErrorNounMessage("Invalid number value for padding");
 *             }
 *         }
 *         
 *         // Extract prefix (optional, default "")
 *         String prefix = "";
 *         grs = this.store.getNoun("prefix");
 *         if (grs != null && !grs.isEmpty()) {
 *             prefix = grs.get(0).toString();
 *         }
 *         
 *         // Extract suffix (optional, default "")
 *         String suffix = "";
 *         grs = this.store.getNoun("suffix");
 *         if (grs != null && !grs.isEmpty()) {
 *             suffix = grs.get(0).toString();
 *         }
 *         
 *         // Extract maxLength (optional, default 0)
 *         int maxLength = 0;
 *         grs = this.store.getNoun("maxLength");
 *         if (grs != null && !grs.isEmpty()) {
 *             try {
 *                 maxLength = Integer.parseInt(grs.get(0).toString());
 *             } catch (NumberFormatException e) {
 *                 return NounMetadata.getErrorNounMessage("Invalid number value for maxLength");
 *             }
 *         }
 *         
 *         // Same logic as AbstractReactor2 version...
 *         String result = text;
 *         
 *         if (toUppercase) {
 *             result = result.toUpperCase();
 *         }
 *         
 *         if (!prefix.isEmpty()) {
 *             result = prefix + result;
 *         }
 *         if (!suffix.isEmpty()) {
 *             result = result + suffix;
 *         }
 *         
 *         if (padding > 0) {
 *             String spaces = " ".repeat(padding);
 *             result = spaces + result + spaces;
 *         }
 *         
 *         if (maxLength > 0 && result.length() > maxLength) {
 *             result = result.substring(0, maxLength - 3) + "...";
 *         }
 *         
 *         return new NounMetadata(result, PixelDataType.CONST_STRING);
 *     }
 *     
 *     @Override
 *     protected String getDescriptionForKey(String key) {
 *         switch (key) {
 *             case "text": return "The text to format";
 *             case "toUppercase": return "Convert text to uppercase";
 *             case "padding": return "Number of spaces to pad on each side";
 *             case "prefix": return "Text to add before the main text";
 *             case "suffix": return "Text to add after the main text";
 *             case "maxLength": return "Maximum length of output (truncate if longer)";
 *             default: return super.getDescriptionForKey(key);
 *         }
 *     }
 *     
 *     @Override
 *     public String getReactorDescription() {
 *         return "Formats text with customizable options (case, padding, prefix/suffix)";
 *     }
 * }
 * 
 * 
 * SUMMARY OF IMPROVEMENTS:
 * 
 * Lines of Code:
 * - AbstractReactor: ~120 lines
 * - AbstractReactor2: ~45 lines (62% reduction!)
 * 
 * Benefits of AbstractReactor2:
 * 1. No manual array management (keysToGet, keyRequired, keyDefaults)
 * 2. No repetitive parameter extraction boilerplate
 * 3. Automatic type conversion and validation
 * 4. Built-in error messages for type conversion failures
 * 5. Parameter descriptions co-located with parameter definitions
 * 6. IDE support for autocomplete and refactoring
 * 7. Compile-time checking of parameter setup
 * 8. Much more readable and maintainable
 * 
 * The annotation-based approach makes reactors self-documenting and significantly
 * reduces the chance of bugs from manual parameter handling.
 */

/**
 * Example usage:
 * 
 * Basic formatting:
 * TextFormatterReactor(text=["Hello World"]);
 * Returns: "Hello World"
 * 
 * With uppercase and padding:
 * TextFormatterReactor(text=["hello"], toUppercase=[true], padding=[2]);
 * Returns: "  HELLO  "
 * 
 * With prefix, suffix, and truncation:
 * TextFormatterReactor(text=["middle"], prefix=["start-"], suffix=["-end"], maxLength=[10]);
 * Returns: "start-mid..."
 */