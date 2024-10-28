import traceback
#from django.shortcuts import redirect
from flask import Flask, session 

from gaas_gpt_database import DatabaseEngine

class LogManager():      

    def __init__(self, user_id=None, insight_id=None):
        self.user_id = user_id
        self.insight_id = insight_id
         
    def save_logs_to_database(self,exceptions,service_name): 

        dbEngineId = "eb98274a-1e5c-46fb-9423-ce43bb595dad"                   
        error_message = str(exceptions)  
        stack_trace = traceback.format_exc()       
        insight = self.insight_id
        user = self.user_id
    
        try:
            if insight is not None:

                databaseEngine = DatabaseEngine(engine_id = dbEngineId,insight_id = insight) 

                databaseEngine.insertData(
                    query = f'INSERT INTO system_logs '
                        f'(userid, error_message, stack_trace, service_name)'
                        f'VALUES (\'{user}\',\'{error_message}\',\'{stack_trace}\', \'{service_name}\')')           
                
        except Exception as db_exp:           
            print(f"Database error: {db_exp}")
            #print(f"Database error: {test}")
           


    
