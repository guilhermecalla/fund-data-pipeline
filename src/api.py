import pandas as pd
import requests
import logging


class MaraviAPI:
    def __init__(self, username, password, client_id, client_secret):
        self.base_url = "https://tarpon.bluedeck.com.br/api"
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.credentials = None
        self.logger = logging.getLogger("MaraviAPI")

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
        
        try:
            response = requests.post(url, data=data, headers=client_headers)
            response.raise_for_status()
            token_json_response = response.json()
            token_header = {
                "Authorization": f"{token_json_response['token_type']} {token_json_response['access_token']}"
            }
            credentials = {**client_headers, **token_header}
            self.credentials = credentials
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Authentication failed: {str(e)}")
            raise

    def fetch_data(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"

        if not self.credentials:
            self.logger.warning("No credentials available. Attempting to authenticate...")
            try:
                self.authenticate()
            except Exception as e:
                self.logger.error(f"Authentication failed: {str(e)}")
                return pd.DataFrame()  # Return empty DataFrame on auth failure

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
        self.logger.info(f"Starting API requests to {endpoint}")
        
        try:
            # Make the first request outside the loop to check the structure
            response = requests.post(url, headers=self.credentials, json=request_params)
            response.raise_for_status()
            result = response.json()
            
            # Determine the response structure type
            if "prices" in result:
                data_key = "prices"
                # For the prices endpoint, we might not need pagination
                if result.get(data_key):
                    all_data.extend(result[data_key])
                    
                # If this endpoint doesn't support pagination, return immediately
                self.logger.info(f"Found {len(all_data)} records from prices endpoint")
                return pd.DataFrame(all_data) if all_data else pd.DataFrame()
                
            elif "objects" in result:
                data_key = "objects"
                if result.get(data_key):
                    all_data.extend(result[data_key].values())
                    
                # Continue with pagination for objects endpoints
                page = 1  # Start with page 1 since we already processed page 0
                
                while True:
                    # Update the page number
                    request_params["pagination"]["page"] = page
                    
                    # Print to debug
                    self.logger.info(f"Fetching page {page}...")
                    
                    # Make the request
                    response = requests.post(url, headers=self.credentials, json=request_params)
                    response.raise_for_status()
                    result = response.json()
                    
                    # Check if we got any data
                    if not result.get(data_key) or len(result[data_key]) == 0:
                        self.logger.info(f"No more data found at page {page}")
                        break
                        
                    # Add the data and increment the page
                    all_data.extend(result[data_key].values())
                    self.logger.info(f"Found {len(result[data_key])} records on page {page}")
                    page += 1
            
            else:
                # Unknown response structure
                self.logger.warning(f"Unknown response structure: {list(result.keys())}")
                return pd.DataFrame()  # Return empty DataFrame for unknown structure
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error: {str(e)}")
            if e.response.status_code == 401:  # Unauthorized
                self.logger.info("Token may have expired. Attempting to reauthenticate...")
                try:
                    self.authenticate()
                    # Try again with fresh credentials
                    return self.fetch_data(endpoint, params)
                except Exception as auth_error:
                    self.logger.error(f"Reauthentication failed: {str(auth_error)}")
            return pd.DataFrame()  # Return empty DataFrame on error
            
        except Exception as e:
            self.logger.error(f"Error fetching data from API: {str(e)}")
            return pd.DataFrame()  # Return empty DataFrame on error
        
        # Return all collected data
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()