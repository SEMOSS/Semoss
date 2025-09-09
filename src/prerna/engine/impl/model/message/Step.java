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
    
    // Setters
    public void setDescription(String description) {
		this.description = description;
	}

	public void setType(StepType type) {
		this.type = type;
	}

	public void setProvenance(Map<String, Object> provenance) {
		this.provenance = provenance == null ? new HashMap<>() : new HashMap<>(provenance);
	}
	
	public void addProvenances(Map<String, Object> provenance) {
		if	(provenance != null) {
			this.provenance.putAll(provenance);
		}
	}
	
    public void addProvenance(String key, Object value) {
        this.provenance.put(key, value);
    }

	public void setRationale(String rationale) {
		this.rationale = rationale;
	}

	//TODO: this should be a deep copy, not a shallow copy
	public void setSuccessCriteria(SuccessCriteria successCriteria) {
		this.successCriteria = successCriteria;
	}

	public void setDetails(Map<String, Object> details) {
		this.details = details == null ? new HashMap<>() : new HashMap<>(details);
	}
	
	public void addDetails(Map<String, Object> details) {	
		if  (this.details == null) {
			this.details = new HashMap<>();
		}
		if	(details != null) {
			this.details.putAll(details);
		}
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

    // Step Builder class
    public static class Builder {
        private final Step step = new Step();
        
        public Builder description(String description) {
            step.description = description;
            return this;
        }

        public Builder type(StepType type) {
            step.type = type;
            return this;
        }

        public Builder withProvenances(Map<String, Object> provenance) {
            step.provenance = provenance == null ? new HashMap<>() : new HashMap<>(provenance);
            return this;
        }
        
        public Builder withProvenance(String key, Object value) {
            if (step.provenance == null) {
                step.provenance = new HashMap<>();
            }
            step.provenance.put(key, value);
            return this;
        }

        public Builder withRationale(String rationale) {
            step.setRationale(rationale);
            return this;
        }

        public Builder withSuccessCriteria(SuccessCriteria successCriteria) {
            step.setSuccessCriteria(successCriteria);
            return this;
        }

        public Builder withDetails(Map<String, Object> details) {
        	step.addDetails(details);
            return this;
        }
        
        public Builder withDetail(String key, Object value) {
        	Map<String, Object> details = new HashMap<>();
        	details.put(key, value);
        	step.addDetails(details);
            return this;
        }
        
        public static Builder fromResponseMessage(ResponseMessage responseMessage, String stepName) {
            // TODO: Parse JSON content using Gson, navigate to execution_plan.steps.{stepName}, and populate Step fields
            
            Map<String, Object> jsonMap = GSON.fromJson(responseMessage.getContent(), new TypeToken<Map<String, Object>>() {}.getType());

            
            Builder stepBuilder = new Builder();
            
            SuccessCriteria.Builder criteriaBuilder = SuccessCriteria.builder();
            
            return stepBuilder;
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
        
        // Setters
        public void setEvaluationLogic(EvaluationLogic evaluationLogic) {
            this.evaluationLogic = evaluationLogic;
        }
        
        public void setConditions(List<Map<String, Object>> conditions) {
        	
            this.conditions = conditions == null ? new ArrayList<>() : new ArrayList<>(conditions);
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
            
            public Builder withEvaluationLogic(EvaluationLogic logic) {
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
}
