import pandas as pd
import requests


class MaraviAPI:
    def __init__(self, username, password, client_id, client_secret):
        self.base_url = "https://tarpon.bluedeck.com.br/api"
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret

        self.credentials = None

    def authenticate(self):
        url = f"{self.base_url}/auth/token"
        data = {
            "username": self.username,
            "password": self.password,
        }
        client_headers = {
            "CF-Access-Client-Id": self.client_id,
            "CF-Access-Client-Secret": self.client_secret,
        }
        response = requests.post(url, data=data, headers=client_headers)

        response.raise_for_status()
        token_json_response = response.json()
        token_header = {
            "Authorization": f"{token_json_response['token_type']} {token_json_response['access_token']}"
        }
        credentials = {**client_headers, **token_header}
        self.credentials = credentials

    def fetch_data(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"

        all_data = []
        page = 0
        if params is None:
            params = {}
            
        # Create a copy of params to avoid modifying the original
        request_params = params.copy()
        request_params["pagination"] = {
            "per_page": 10000,  # Número de itens por página
            "page": 0,  # Página inicial
        }

        # For debugging
        print(f"Starting API requests to {endpoint}")
        
        # Make the first request outside the loop to check the structure
        response = requests.post(url, headers=self.credentials, json=request_params, timeout=60)
        response.raise_for_status()
        result = response.json()
        
        # Handle operations data structure (objects endpoint)
        if "objects" in result:
            data_key = "objects"
            if result.get(data_key):
                all_data.extend(result[data_key].values())
                
            # Continue with pagination for objects endpoints
            page = 1  # Start with page 1 since we already processed page 0
            
            while True:
                # Update the page number
                request_params["pagination"]["page"] = page
                
                # Print to debug
                print(f"Fetching page {page}...")
                
                # Make the request
                response = requests.post(url, headers=self.credentials, json=request_params)
                response.raise_for_status()
                result = response.json()
                
                # Check if we got any data
                if not result.get(data_key) or len(result[data_key]) == 0:
                    print(f"No more data found at page {page}")
                    break
                    
                # Add the data and increment the page
                all_data.extend(result[data_key].values())
                print(f"Found {len(result[data_key])} records on page {page}")
                page += 1
                
                # Safety break to avoid infinite loops
                if page > 100:
                    print(f"Safety break at page {page}")
                    break
        
        else:
            # Unknown response structure
            print(f"Unknown response structure: {list(result.keys())}")
        
        # Return all collected data
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()