package prerna.engine.impl.model.message;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import com.google.gson.annotations.SerializedName;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

public class Step {
    
    private static final Gson GSON = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .disableHtmlEscaping()
            .create();
	
    public enum StepType {
        @SerializedName("tool_call")
        TOOL_CALL,
        @SerializedName("llm_reasoning")
        LLM_REASONING,
        @SerializedName("human_intervention") 
        HUMAN_INTERVENTION,
        @SerializedName("no_tool_available")
        NO_TOOL_AVAILABLE
    }
    
    // Required fields from schema
    @SerializedName("description")
    private String description;
    
    @SerializedName("type")
    private StepType type;
    
    @SerializedName("provenance")
    private Map<String, Object> provenance;
    
    @SerializedName("rationale")
    private String rationale;
    
    @SerializedName("success_criteria")
    private SuccessCriteria successCriteria;
    
    @SerializedName("details")
    private Map<String, Object> details;

    // Private constructor - use Builder
    private Step() {
        this.provenance = new HashMap<>();
        this.details = new HashMap<>();
    }
    
    // Getters
    public String getDescription() {
        return description;
    }

    public StepType getType() {
        return type;
    }

    public Map<String, Object> getProvenance() {
        return provenance == null ? new HashMap<>() : new HashMap<>(provenance);
    }

    public String getRationale() {
        return rationale;
    }

    public SuccessCriteria getSuccessCriteria() {
        return successCriteria;
    }

    public Map<String, Object> getDetails() {
        return details == null ? new HashMap<>() : new HashMap<>(details);
    }

    public static Builder builder() {
        return new Builder();
    }

    // Inner SuccessCriteria class
    public static class SuccessCriteria {
        
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
        private SuccessCriteria() {
            this.conditions = new ArrayList<>();
        }
        
        // Getters
        public EvaluationLogic getEvaluationLogic() {
            return evaluationLogic;
        }
        
        public List<Map<String, Object>> getConditions() {
            return conditions == null ? new ArrayList<>() : new ArrayList<>(conditions);
        }
        
        public static Builder builder() {
            return new Builder();
        }
        
        public static class Builder {
            private final SuccessCriteria criteria = new SuccessCriteria();
            
            public Builder evaluationLogic(EvaluationLogic logic) {
                criteria.evaluationLogic = logic;
                return this;
            }
            
            public Builder conditions(List<Map<String, Object>> conditions) {
                criteria.conditions = conditions == null ? new ArrayList<>() : new ArrayList<>(conditions);
                return this;
            }
            
            public Builder addCondition(Map<String, Object> condition) {
                if (criteria.conditions == null) {
                    criteria.conditions = new ArrayList<>();
                }
                criteria.conditions.add(condition);
                return this;
            }
            
            public SuccessCriteria build() {
                if (criteria.evaluationLogic == null) {
                    throw new IllegalStateException("evaluation_logic is required");
                }
                if (criteria.conditions == null || criteria.conditions.isEmpty()) {
                    throw new IllegalStateException("conditions is required and cannot be empty");
                }
                return criteria;
            }
        }
    }

    // Step Builder class
    public static class Builder {
        private final Step step = new Step();

        public static Builder buildFromResponseMessage(ResponseMessage responseMessage, String stepName) {
            // TODO: Parse JSON content using Gson, navigate to execution_plan.steps.{stepName}, and populate Step fields
            
            Map<String, Object> jsonMap = GSON.fromJson(responseMessage.getContent(), new TypeToken<Map<String, Object>>() {}.getType());

            
            Builder stepBuilder = new Builder();
            
            SuccessCriteria.Builder criteriaBuilder = SuccessCriteria.builder();
            
            return stepBuilder;
        }
        
        
        public Builder description(String description) {
            step.description = description;
            return this;
        }

        public Builder type(StepType type) {
            step.type = type;
            return this;
        }

        public Builder provenance(Map<String, Object> provenance) {
            step.provenance = provenance == null ? new HashMap<>() : new HashMap<>(provenance);
            return this;
        }
        
        public Builder addProvenance(String key, Object value) {
            if (step.provenance == null) {
                step.provenance = new HashMap<>();
            }
            step.provenance.put(key, value);
            return this;
        }

        public Builder rationale(String rationale) {
            step.rationale = rationale;
            return this;
        }

        public Builder successCriteria(SuccessCriteria successCriteria) {
            step.successCriteria = successCriteria;
            return this;
        }

        public Builder details(Map<String, Object> details) {
            step.details = details == null ? new HashMap<>() : new HashMap<>(details);
            return this;
        }
        
        public Builder addDetail(String key, Object value) {
            if (step.details == null) {
                step.details = new HashMap<>();
            }
            step.details.put(key, value);
            return this;
        }

        public Step build() {
            // Validate required fields from schema
            if (step.description == null || step.description.trim().isEmpty()) {
                throw new IllegalStateException("description is required");
            }
            if (step.type == null) {
                throw new IllegalStateException("type is required");
            }
            if (step.provenance == null) {
                step.provenance = new HashMap<>();
            }
            if (step.rationale == null || step.rationale.trim().isEmpty()) {
                throw new IllegalStateException("rationale is required");
            }
            if (step.successCriteria == null) {
                throw new IllegalStateException("success_criteria is required");
            }
            if (step.details == null) {
                step.details = new HashMap<>();
            }
            return step;
        }
    }
}
