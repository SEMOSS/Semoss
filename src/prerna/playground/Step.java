package prerna.playground;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.engine.impl.model.message.ResponseMessage;

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
	
	public void addProvenance(Map<String, Object> provenance) {
    	if (this.provenance == null) {
    		this.provenance = new HashMap<String, Object>();
    	}
		if	(provenance != null) {
			this.provenance.putAll(provenance);
		}
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
    	if (this.details == null) {
    		this.details = new HashMap<String, Object>();
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
        
        public Builder withDescription(String description) {
            step.setDescription(description);
            return this;
        }

        public Builder withType(StepType type) {
            step.setType(type);
            return this;
        }

        public Builder withProvenance(Map<String, Object> provenance) {
        	step.addProvenance(provenance);
            return this;
        }
        

        public Builder withRationale(String rationale) {
            step.setRationale(rationale);
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
        
        public Builder withSuccessCriteria(SuccessCriteria successCriteria) {
            step.setSuccessCriteria(successCriteria);
            return this;
        }
        
        //SuccessCriteria Components
        public Builder withEvaluationLogic(SuccessCriteria.EvaluationLogic evaluationLogic) {
        	if(step.successCriteria == null) {
        		step.successCriteria = new SuccessCriteria();
        	}
            step.successCriteria.setEvaluationLogic(evaluationLogic);
            return this;
        }
        
        public Builder withConditions(List<Map<String, Object>> conditions) {
            step.successCriteria.addConditions(conditions);
            return this;
        }
        
        public Builder withCondition(Map<String, Object> condition) {
        	ArrayList<Map<String, Object>> conditions = new ArrayList<>();
        	conditions.add(condition);
            step.successCriteria.addConditions(conditions);
            return this;
        }
        
        // TODO: Parse JSON content using Gson, navigate to execution_plan.steps.{stepName}, and populate Step fields
        public static Builder fromResponseMessage(ResponseMessage responseMessage, String stepName) {
            //INCOMPLETE METHOD
            Map<String, Object> jsonMap = GSON.fromJson(responseMessage.getContent(), new TypeToken<Map<String, Object>>() {}.getType());

            
            Builder stepBuilder = new Builder();
            
            return stepBuilder;
        }

        public Step build() {
            if (step.description == null || step.description.trim().isEmpty()) {
                throw new IllegalStateException("description is required");
            }
            if (step.type == null) {
                throw new IllegalStateException("type is required");
            }
            if (step.rationale == null || step.rationale.trim().isEmpty()) {
                throw new IllegalStateException("rationale is required");
            }
            if (step.successCriteria == null) {
                throw new IllegalStateException("success_criteria is required");
            }
            return step;
        }
    }
}
