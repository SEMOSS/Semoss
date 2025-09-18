package prerna.playground;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.gson.annotations.SerializedName;

public class SuccessCriteria {
    
    public enum EvaluationLogic {
        @SerializedName("ALL")
        ALL,
        @SerializedName("ANY") 
        ANY
    }
    
    @SerializedName("evaluation_logic")
    private EvaluationLogic evaluationLogic;
    
    @SerializedName("conditions")
    private List<Map<String, Object>> conditions;
    
    // Private constructor
    public SuccessCriteria() {
        this.conditions = new ArrayList<>();
    }
    
    // Setters
    public void setEvaluationLogic(EvaluationLogic evaluationLogic) {
        this.evaluationLogic = evaluationLogic;
    }
    
    public void setConditions(List<Map<String, Object>> conditions) {
        this.conditions = conditions == null ? new ArrayList<>() : new ArrayList<>(conditions);
    }
    
    public void addConditions(List<Map<String, Object>> conditions) {
    	if (this.conditions == null) {
    		this.conditions = new ArrayList<Map<String, Object>>();
    	}
    	if (conditions != null) {
    		this.conditions.addAll(conditions);
    	}
    }
    
    // Getters
    public EvaluationLogic getEvaluationLogic() {
        return evaluationLogic;
    }
    
    public List<Map<String, Object>> getConditions() {
        return conditions == null ? new ArrayList<>() : new ArrayList<>(conditions);
    }
}
