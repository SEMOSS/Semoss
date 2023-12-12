from typing import Dict

def google_initializer(
    region:str,
    service_account_credentials:Dict = None,
    service_account_key_file:str = None,
    project:str = None
):
    '''
    Initialize the connection via the aiplatform sdk.
    
    Args:
        model_name(`str`):
            The name of the chat, code or embeddings model
        region(`str`):
            The region where the project is hosted
        service_account_credentials(`Dict`):
            A python dictionary containing the servive account information
        service_account_key_file(`str`):
            A file path to a json file containing the service account information

    Returns:
        `google.cloud.aiplatform.initializer._Config`
        Stores common parameters and options for API calls.
    '''
    from google.oauth2 import service_account
    from google.cloud import aiplatform
    
    service_account_info = service_account_credentials or service_account_key_file
    if not (isinstance(service_account_info, Dict)):
        import json
        with open(service_account_info) as credentials_file:
            service_account_info = json.load(credentials_file)
            
    service_account_credentials = service_account.Credentials.from_service_account_info(
        service_account_info
    )

    project = project or \
                service_account_info.get('project_id') or \
                service_account_info.get('projectId') or \
                service_account_info.get('projectID') or \
                service_account_info.get('project')
                    
    aiplatform.init(
        project=project,
        location=region,
        credentials=service_account_credentials,
    )        