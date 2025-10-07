package prerna.reactor.examples;

import java.util.ArrayList;
import java.util.List;

import prerna.reactor.AbstractReactor2;
import prerna.reactor.annotations.ReactorKey;
import prerna.reactor.annotations.ReactorOutput;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * Advanced example showing mathematical operations with multiple data types
 * 
 * This reactor performs statistical calculations on a list of numbers.
 * Demonstrates:
 * - Multiple data types (numbers, booleans, strings)
 * - Multi-value parameters (arrays)
 * - Default values and optional parameters
 * - Input validation
 * - Error handling
 */
@ReactorOutput(
    description = "Calculates statistics (sum, average, min, max) for a list of numbers",
    dataType = PixelDataType.CONST_STRING
)
public class StatisticsReactor extends AbstractReactor2 {

    @ReactorKey(
        key = "numbers", 
        description = "List of numbers to calculate statistics for", 
        required = true,
        multi = true,
        dataType = PixelDataType.CONST_DECIMAL
    )
    private List<Double> numbers;

    @ReactorKey(
        key = "includeSum", 
        description = "Whether to include sum in the output", 
        defaultValue = "true",
        dataType = PixelDataType.BOOLEAN
    )
    private Boolean includeSum;

    @ReactorKey(
        key = "includeAverage", 
        description = "Whether to include average in the output", 
        defaultValue = "true",
        dataType = PixelDataType.BOOLEAN
    )
    private Boolean includeAverage;

    @ReactorKey(
        key = "includeMinMax", 
        description = "Whether to include min/max in the output", 
        defaultValue = "true",
        dataType = PixelDataType.BOOLEAN
    )
    private Boolean includeMinMax;

    @ReactorKey(
        key = "precision", 
        description = "Number of decimal places for results", 
        defaultValue = "2",
        dataType = PixelDataType.CONST_INT
    )
    private Integer precision;

    @ReactorKey(
        key = "format", 
        description = "Output format: 'text' or 'json'", 
        defaultValue = "text",
        dataType = PixelDataType.CONST_STRING
    )
    private String format;

    @Override
    public NounMetadata executeReactor() throws SemossPixelException {
        // Validate inputs
        if (numbers == null || numbers.isEmpty()) {
            throw new SemossPixelException("Numbers list cannot be empty");
        }
        
        // Remove any null values
        List<Double> validNumbers = new ArrayList<>();
        for (Double num : numbers) {
            if (num != null) {
                validNumbers.add(num);
            }
        }
        
        if (validNumbers.isEmpty()) {
            throw new SemossPixelException("No valid numbers provided");
        }
        
        // Calculate statistics
        double sum = 0;
        double min = validNumbers.get(0);
        double max = validNumbers.get(0);
        
        for (Double num : validNumbers) {
            sum += num;
            if (num < min) min = num;
            if (num > max) max = num;
        }
        
        double average = sum / validNumbers.size();
        
        // Format results
        String formatStr = "%." + precision + "f";
        
        if ("json".equalsIgnoreCase(format)) {
            // Return JSON format
            StringBuilder json = new StringBuilder();
            json.append("{");
            json.append("\"count\":").append(validNumbers.size());
            
            if (includeSum) {
                json.append(",\"sum\":").append(String.format(formatStr, sum));
            }
            if (includeAverage) {
                json.append(",\"average\":").append(String.format(formatStr, average));
            }
            if (includeMinMax) {
                json.append(",\"min\":").append(String.format(formatStr, min));
                json.append(",\"max\":").append(String.format(formatStr, max));
            }
            
            json.append("}");
            return new NounMetadata(json.toString(), PixelDataType.CONST_STRING);
            
        } else {
            // Return text format
            StringBuilder result = new StringBuilder();
            result.append("Statistics for ").append(validNumbers.size()).append(" numbers:\n");
            
            if (includeSum) {
                result.append("Sum: ").append(String.format(formatStr, sum)).append("\n");
            }
            if (includeAverage) {
                result.append("Average: ").append(String.format(formatStr, average)).append("\n");
            }
            if (includeMinMax) {
                result.append("Min: ").append(String.format(formatStr, min)).append("\n");
                result.append("Max: ").append(String.format(formatStr, max)).append("\n");
            }
            
            return new NounMetadata(result.toString().trim(), PixelDataType.CONST_STRING);
        }
    }
}

/**
 * Example usage:
 * 
 * Basic statistics:
 * StatisticsReactor(numbers=[1.5, 2.3, 4.7, 3.1, 5.9]);
 * Returns: "Statistics for 5 numbers:
 *           Sum: 17.50
 *           Average: 3.50
 *           Min: 1.50
 *           Max: 5.90"
 * 
 * JSON format with custom precision:
 * StatisticsReactor(numbers=[10, 20, 30], format=["json"], precision=[0]);
 * Returns: {"count":3,"sum":"60","average":"20","min":"10","max":"30"}
 * 
 * Selective statistics:
 * StatisticsReactor(numbers=[1,2,3,4,5], includeSum=[false], includeMinMax=[false]);
 * Returns: "Statistics for 5 numbers:
 *           Average: 3.00"
 */