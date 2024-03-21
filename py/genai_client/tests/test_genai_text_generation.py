### ATTENTION 
### To run this you need to be in the Semoss_Dev/py directory
### Then run the following command
### py -3.10 -m unittest genai_client.tests.test_genai_text_generation

import unittest

from typing import Dict

from genai_client import (
    OpenAiClient,
    AzureOpenAiClient,
    TextGenClient,
    BedrockClient,
    VertexClient,
)

from genai_client.constants import (
    AskModelEngineResponse
)

FULL_PROMPT = [
    {
        "role": "system",
        "content": "You are a helpful, pattern-following assistant that translates corporate jargon into plain English.",
    },
    {
        "role": "system",
        "content": "New synergies will help drive top-line growth.",
    },
    {
        "role": "system",
        "content": "Things working well together will increase revenue.",
    },
    {
        "role": "system",
        "content": "Let's circle back when we have more bandwidth to touch base on opportunities for increased leverage.",
    },
    {
        "role": "system",
        "content": "Let's talk later when we're less busy about how to do better.",
    },
    {
        "role": "user",
        "content": "This late pivot means we don't have time to boil the ocean for the client deliverable.",
    },
]

SAMPLE_QUESTION = 'What is the capital of France?'

class AskModelTests(unittest.TestCase):
        
    def test_text_generation_inference(self):
        # declare the model
        model = TextGenClient(
            endpoint="***REMOVED***",
            model_name="garage-bAInd/Platypus2-70B-instruct",
            max_tokens=8192,
            max_input_tokens=4096,
        )

        # make sure the ask expected
        ask_response = model.ask_call(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, AskModelEngineResponse)
        self.assertCountEqual(ask_response.to_dict(), ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = [SAMPLE_QUESTION])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        
        
        
    def test_openai_chat_completions(self):
        # declare the model
        model = OpenAiClient(
            model_name="gpt-3.5-turbo",
            api_key="***REMOVED***",
            max_tokens=4097,
            chat_type='chat-completion'
        )

        # make sure the ask expected
        ask_response = model.ask(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        
    def test_openai_completions(self):
        # declare the model
        model = OpenAiClient(
            model_name="gpt-3.5-turbo-instruct",
            api_key="***REMOVED***",
            max_tokens=4097,
            chat_type='completions'
        )

        # make sure the ask expected
        ask_response = model.ask(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        
    def test_azure_chat_completions(self):
        # declare the model
        model = AzureOpenAiClient(
            model_name="-gpt-35-turbo",
            api_key="***REMOVED***",
            endpoint="***REMOVED***",
            max_tokens=4097,
        )

        # make sure the ask expected
        ask_response = model.ask(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

    # TODO dont have a model to test with
    # def test_azure_completions(self):
    #     # declare the model
    #     model = AzureOpenAiClient(
    #         model_name="-gpt-35-turbo",
    #         api_key="***REMOVED***",
    #         endpoint="***REMOVED***",
    #         max_tokens=4097,
    #     )

    #     # make sure the ask expected
    #     ask_response = model.ask(question = SAMPLE_QUESTION)
    #     self.assertIsInstance(ask_response, Dict)
    #     self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

    #     embeddings_response = model.embeddings(strings_to_embed = ['hello'])
    #     self.assertIsInstance(embeddings_response, Dict)
    #     self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
    
    def test_aws_bedrock_claude(self):
        # declare the model
        model = BedrockClient(
            modelId="anthropic.claude-instant-v1",
            secret_key="***REMOVED***",
            access_key="***REMOVED***",
            region="us-east-1",
        )
        
        # make sure the ask expected
        ask_response = model.ask_call(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, AskModelEngineResponse)
        self.assertCountEqual(ask_response.to_dict(), ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

    def test_vertex_text_bison(self):
        # declare the model
        model = VertexClient(
            model_name="text-bison",
            chat_type="text",
            service_account_key_file="C:\\Users\\ttrankle\\Documents\\Semoss\\CFG.AI\\us-gcp-ame-adv-a66-npd-1-sa 2.json",
            region="us-central1",
        )
        
        # make sure the ask expected
        ask_response = model.ask(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        
          
    def test_vertex_chat_bison(self):
        # declare the model
        model = VertexClient(
            model_name="chat-bison",
            chat_type="chat",
            service_account_key_file="C:\\Users\\ttrankle\\Documents\\Semoss\\CFG.AI\\us-gcp-ame-adv-a66-npd-1-sa 2.json",
            region="us-central1",
        )
        
        # make sure the ask expected
        ask_response = model.ask(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        
    def test_vertex_gemini(self):
        # declare the model
        model = VertexClient(
            model_name="gemini-pro",
            chat_type="generative",
            service_account_key_file="C:\\Users\\ttrankle\\Documents\\Semoss\\CFG.AI\\us-gcp-ame-adv-a66-npd-1-sa 2.json",
            region="us-central1",
        )
        
        # make sure the ask expected
        ask_response = model.ask(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        
    def test_vertex_code_bison(self):
        # declare the model
        model = VertexClient(
            model_name="code-bison",
            chat_type="code",
            service_account_key_file="C:\\Users\\ttrankle\\Documents\\Semoss\\CFG.AI\\us-gcp-ame-adv-a66-npd-1-sa 2.json",
            region="us-central1",
        )
        
        # make sure the ask expected
        ask_response = model.ask(question = SAMPLE_QUESTION)
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        
    def test_vertex_codechat_bison(self):
        # declare the model
        model = VertexClient(
            model_name="codechat-bison",
            chat_type="codechat",
            service_account_key_file="C:\\Users\\ttrankle\\Documents\\Semoss\\CFG.AI\\us-gcp-ame-adv-a66-npd-1-sa 2.json",
            region="us-central1",
        )
        
        # make sure the ask expected
        ask_response = model.ask(question = "Please help write a function to calculate the min of two numbers")
        self.assertIsInstance(ask_response, Dict)
        self.assertCountEqual(ask_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])

        embeddings_response = model.embeddings(strings_to_embed = ['hello'])
        self.assertIsInstance(embeddings_response, Dict)
        self.assertCountEqual(embeddings_response, ['response', 'numberOfTokensInPrompt', 'numberOfTokensInResponse'])
        