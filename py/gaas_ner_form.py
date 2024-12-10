### haystack
##
#!pip install transformers
#
# Models - T5
#
# ONLY FOR COLAB - !pip install Ipython --upgrade
# %load_ext autoreload
# %autoreload 2

# input_data - sentence that the user provides
# form_fields - the fields in the form that needs to be filled
# FillForm ( command = "'patient dodo ramalingam with a large frame has been injured in an accident. patient is male. He is living in singapore for the last 2 years'" , fields = [ 'first name' , 'last name' ] ) ;


def search_form_fields(
    input_data=None,
    form_fields=None,
    ckpt="google/flan-t5-base",
    answer_key="generated_text",
    pipe=None,
):

    # Need to centralize this
    responses = {}
    unknown_responses = []

    for field in form_fields:
        text = f"question: {field}? context: {input_data}"
        response = pipe(text)[0]
        field_response = response[answer_key]
        if field_response != "is not available":  # we got out response
            responses.update({field: field_response})
        else:
            responses.update({field: f"What about {field} ?"})
            unknown_responses.append(field)
    return responses


def init(ckpt="google/flan-t5-base"):
    from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline

    t5_model = AutoModelForSeq2SeqLM.from_pretrained(ckpt)
    t5_tokenizer = AutoTokenizer.from_pretrained(ckpt)
    pipe = pipeline("text2text-generation", model=t5_model, tokenizer=t5_tokenizer)
    return pipe
