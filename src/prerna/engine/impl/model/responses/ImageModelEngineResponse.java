package prerna.engine.impl.model.responses;

import java.util.Map;

public class ImageModelEngineResponse extends AbstractImageModelEngineResponse<String> {

    private static final long serialVersionUID = 1L;

    public ImageModelEngineResponse(
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
        super(file_path, generation_time, seed, prompt, negative_prompt, guidance_scale, num_inference_steps, height, width, model_name, vae_model_name);
    }

    public static ImageModelEngineResponse fromMap(Map<String, Object> modelResponse) {
        String file_path = (String) modelResponse.get(FILE_PATH);
        Integer generation_time = getInteger(modelResponse.get(GENERATION_TIME));
        String seed = (String) modelResponse.get(SEED);
        String prompt = (String) modelResponse.get(PROMPT);
        String negative_prompt = (String) modelResponse.get(NEGATIVE_PROMPT);
        Double guidance_scale = (Double) modelResponse.get(GUIDANCE_SCALE);
        Integer num_inference_steps = getInteger(modelResponse.get(NUM_INFERENCE_STEPS));
        Integer height = getInteger(modelResponse.get(HEIGHT));
        Integer width = getInteger(modelResponse.get(WIDTH));
        String model_name = (String) modelResponse.get(MODEL_NAME);
        String vae_model_name = (String) modelResponse.get(VAE_MODEL_NAME);

        return new ImageModelEngineResponse(file_path, generation_time, seed, prompt, negative_prompt, guidance_scale, num_inference_steps, height, width, model_name, vae_model_name);
    }

    @SuppressWarnings("unchecked")
    public static ImageModelEngineResponse fromObject(Object responseObject) {
        Map<String, Object> modelResponse = (Map<String, Object>) responseObject;
        return fromMap(modelResponse);
    }
}
