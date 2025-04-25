from gaas_gpt_model import ModelEngine

def interpret_code(code_example: str,insight_id:str) -> str:
    model = ModelEngine(engine_id = "4801422a-5c62-421e-a00c-05c6a9e15de8", insight_id = insight_id)
    question = 'Interpret the following code example :'+code_example
    output = model.ask(question = question)
    if isinstance(output, list) and len(output) > 0 and 'response' in output[0]:         
        return output[0]['response']    
    else:         
        raise ValueError("Unexpected output format from model.ask()")
