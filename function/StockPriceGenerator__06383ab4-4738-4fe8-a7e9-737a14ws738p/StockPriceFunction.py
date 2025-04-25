# from ai_server import ServerClient
# from ai_server import ModelEngine

# #gov connect
# accessKey = "0b24f7a4-6de0-4f15-9ab6-36d6dae743b6"
# secretKey = "2485ff82-550b-4e50-b442-a5306ea386a4"
# base = "https://govconnectai.deloitte.com/gcai-dev/Monolith/api"
 
# loginKeys = {"secretKey":secretKey,"accessKey":accessKey,"base":base}
# server_connection = ServerClient(
# access_key=loginKeys['accessKey'],
# secret_key=loginKeys['secretKey'],
# base=loginKeys['base'])

import json
import urllib.request
import requests

from gaas_gpt_model import ModelEngine

# model = ModelEngine(engine_id = "87616874-b6f6-41e8-9ad9-12abac8ac950", insight_id = server_connection.cur_insight)



def get_stock_price(api_key, company_name, start_date, end_date , insight_id):
    # Use Polygon's API to fetch the ticker symbol dynamically
    model = ModelEngine(engine_id = "4801422a-5c62-421e-a00c-05c6a9e15de8", insight_id = insight_id)
    ticker_url = f"https://api.polygon.io/v3/reference/tickers"
    ticker_params = {
        'search': company_name,
        'active': 'true',
        'apiKey': api_key
    }
    ticker_response = requests.get(ticker_url, params=ticker_params)
    if ticker_response.status_code == 200:
        ticker_data = ticker_response.json()
        if 'results' in ticker_data and len(ticker_data['results']) > 0:
            ticker_symbol = ticker_data['results'][0]['ticker']
        else:
            return f"Error: Could not find ticker symbol for {company_name}"
    else:
        return f"Error: {ticker_response.status_code} - {ticker_response.text}"

    # Fetch stock price data using the ticker symbol
    stock_url = f"https://api.polygon.io/v2/aggs/ticker/{ticker_symbol}/range/1/day/{start_date}/{end_date}"
    stock_params = {'apiKey': api_key}
    stock_response = requests.get(stock_url, params=stock_params)
    if stock_response.status_code == 200:
        stock_data = stock_response.json()
        return stock_data
    else:
        return f"Error: {stock_response.status_code} - {stock_response.text}"


# if __name__ == "__main__":
#     polygon_api_key = "iEVSMvP6pjmPZv0JEB2UbjLT3W1iwOkM"
#     company_name = "Apple"
#     start_date = "2023-01-01"
#     end_date = "2023-01-31"
#     insight_id = "d60599a3-5e42-4039-8b66-41f7051cef67"
    
#     stock_data = get_stock_price(polygon_api_key, company_name, start_date, end_date, insight_id)
#     print(stock_data)