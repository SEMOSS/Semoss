package prerna.engine.impl.model.responses;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstractImageModelEngineResponse<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String FILE_PATH = "file_path";
    public static final String GENERATION_TIME = "generation_time";
    public static final String SEED = "seed";
    public static final String PROMPT = "prompt";
    public static final String NEGATIVE_PROMPT = "negative_prompt";
    public static final String GUIDANCE_SCALE = "guidance_scale";
    public static final String NUM_INFERENCE_STEPS = "num_inference_steps";
    public static final String HEIGHT = "height";
    public static final String WIDTH = "width";
    public static final String MODEL_NAME = "model_name";
    public static final String VAE_MODEL_NAME = "vae_model_name";

    private String file_path;
    private Integer generation_time;
    private String seed;
    private String prompt;
    private String negative_prompt;
    private Double guidance_scale;
    private Integer num_inference_steps;
    private Integer height;
    private Integer width;
    private String model_name;
    private String vae_model_name;

    public AbstractImageModelEngineResponse(
    		String file_path, 
    		Integer generation_time, 
    		String seed, 
    		String prompt, 
    		String negative_prompt, 
    		Double guidance_scale, 
    		Integer num_inference_steps, 
    		Integer height, 
    		Integer width, 
    		String model_name, 
    		String vae_model_name
    		) {
        this.file_path = file_path;
        this.generation_time = generation_time;
        this.seed = seed;
        this.prompt = prompt;
        this.negative_prompt = negative_prompt;
        this.guidance_scale = guidance_scale;
        this.num_inference_steps = num_inference_steps;
        this.height = height;
        this.width = width;
        this.model_name = model_name;
        this.vae_model_name = vae_model_name;
    }

    public String getFilePath() {
        return file_path;
    }

    public void setFilePath(String file_path) {
        this.file_path = file_path;
    }

    public Integer getGenerationTime() {
        return generation_time;
    }

    public void setGenerationTime(Integer generation_time) {
        this.generation_time = generation_time;
    }

    public String getSeed() {
        return seed;
    }

    public void setSeed(String seed) {
        this.seed = seed;
    }

    public String getPrompt() {
        return prompt;
    }

    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }

    public String getNegativePrompt() {
        return negative_prompt;
    }

    public void setNegativePrompt(String negativePrompt) {
        this.negative_prompt = negativePrompt;
    }

    public Double getGuidanceScale() {
        return guidance_scale;
    }

    public void setGuidanceScale(Double guidance_scale) {
        this.guidance_scale = guidance_scale;
    }

    public Integer getNumInferenceSteps() {
        return num_inference_steps;
    }

    public void setNumInferenceSteps(Integer num_inference_steps) {
        this.num_inference_steps = num_inference_steps;
    }

    public Integer getHeight() {
        return height;
    }

    public void setHeight(Integer height) {
        this.height = height;
    }

    public Integer getWidth() {
        return width;
    }

    public void setWidth(Integer width) {
        this.width = width;
    }

    public String getModelName() {
        return model_name;
    }

    public void setModelName(String model_name) {
        this.model_name = model_name;
    }

    public String getVaeModelName() {
        return vae_model_name;
    }

    public void setVaeModelName(String vae_model_name) {
        this.vae_model_name = vae_model_name;
    }

    public Map<String, Object> toMap() {
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put(FILE_PATH, this.file_path);
        responseMap.put(GENERATION_TIME, this.generation_time);
        responseMap.put(SEED, this.seed);
        responseMap.put(PROMPT, this.prompt);
        responseMap.put(NEGATIVE_PROMPT, this.negative_prompt);
        responseMap.put(GUIDANCE_SCALE, this.guidance_scale);
        responseMap.put(NUM_INFERENCE_STEPS, this.num_inference_steps);
        responseMap.put(HEIGHT, this.height);
        responseMap.put(WIDTH, this.width);
        responseMap.put(MODEL_NAME, this.model_name);
        responseMap.put(VAE_MODEL_NAME, this.vae_model_name);

        return responseMap;
    }

    protected static Integer getInteger(Object value) {
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof Double) {
            return ((Double) value).intValue();
        } else if (value instanceof String) {
            return Integer.valueOf((String) value);
        } else {
            return null;
        }
    }
}
