import pandas as pd
import requests
import logging
import json


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

    def _clean_data_for_postgres(self, position):
        """Remove or convert data types that PostgreSQL can't handle"""
        cleaned_position = {}
        
        for key, value in position.items():
            if isinstance(value, dict):
                # Convert dict to JSON string
                cleaned_position[key] = json.dumps(value) if value else None
            elif isinstance(value, list):
                # Convert list to JSON string
                cleaned_position[key] = json.dumps(value) if value else None
            else:
                cleaned_position[key] = value
                
        return cleaned_position

    def fetch_data(self, endpoint, params=None):
        url = f"{self.base_url}/{endpoint}"

        if not self.credentials:
            self.logger.warning("No credentials available. Attempting to authenticate...")
            try:
                self.authenticate()
            except Exception as e:
                self.logger.error(f"Authentication failed: {str(e)}")
                return pd.DataFrame()

        all_positions = []
        
        if params is None:
            params = {}
            
        request_params = params.copy()
        request_params["pagination"] = {
            "per_page": 10000,
            "page": 0,
        }

        self.logger.info(f"Starting API requests to {endpoint}")
        
        try:
            # Make request
            response = requests.post(url, headers=self.credentials, json=request_params, timeout=30)
            response.raise_for_status()
            result = response.json()
            
            if "objects" in result:
                portfolios = result.get("objects", {})
                
                for portfolio_id, portfolio_data in portfolios.items():
                    # Extract portfolio info
                    portfolio_name = portfolio_data.get("name")
                    portfolio_date = portfolio_data.get("date")
                    
                    # Extract instrument positions
                    instrument_positions = portfolio_data.get("instrument_positions", [])
                    
                    for position in instrument_positions:
                        # Add portfolio info to each position
                        position_with_portfolio = position.copy()
                        position_with_portfolio["portfolio_name"] = portfolio_name
                        position_with_portfolio["portfolio_id"] = portfolio_id
                        position_with_portfolio["date"] = portfolio_date
                        position_with_portfolio["position_type"] = "POSITION"
                        
                        # Clean data for PostgreSQL
                        cleaned_position = self._clean_data_for_postgres(position_with_portfolio)
                        all_positions.append(cleaned_position)
                    
                    # Extract financial transaction positions (provisões)
                    financial_transactions = portfolio_data.get("financial_transaction_positions", [])
                    
                    for transaction in financial_transactions:
                        # Convert provision to position format usando category_name como instrument_name
                        provision_position = {
                            "portfolio_name": portfolio_name,
                            "portfolio_id": portfolio_id,
                            "date": portfolio_date,
                            "instrument_name": transaction.get("category_name"),
                            "quantity": 1,  # Quantidade conceitual
                            "price": transaction.get("financial_value", 0),
                            "asset_value": transaction.get("financial_value", 0),
                            "book_name": transaction.get("book_name"),
                            "position_type": "PROVISION",
                            # Provisões só têm pct_net_asset_value
                            "pct_net_asset_value": transaction.get("pct_net_asset_value", 0),
                            "pct_asset_value": None,  # Provisões não têm esta coluna,
                            "sector_name": "Não utilizar" # Provisões não têm esta coluna
                        }
                        
                        all_positions.append(provision_position)
                
                self.logger.info(f"Extracted {len([p for p in all_positions if p['position_type'] == 'POSITION'])} positions and {len([p for p in all_positions if p['position_type'] == 'PROVISION'])} provisions from {len(portfolios)} portfolios")
                
            else:
                self.logger.warning(f"Expected 'objects' key, but found: {list(result.keys())}")
                return pd.DataFrame()
            
        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error: {str(e)}")
            if e.response.status_code == 401:
                self.logger.info("Token may have expired. Attempting to reauthenticate...")
                try:
                    self.authenticate()
                    return self.fetch_data(endpoint, params)
                except Exception as auth_error:
                    self.logger.error(f"Reauthentication failed: {str(auth_error)}")
            return pd.DataFrame()
            
        except Exception as e:
            self.logger.error(f"Error fetching data from API: {str(e)}")
            return pd.DataFrame()
        
        self.logger.info(f"Total records collected: {len(all_positions)}")
        return pd.DataFrame(all_positions) if all_positions else pd.DataFrame()