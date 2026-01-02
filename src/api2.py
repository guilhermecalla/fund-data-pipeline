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

    def fetch_data(self, endpoint, params=None, key="positions"):
        url = f"{self.base_url}/{endpoint}"

        if not self.credentials:
            self.logger.warning(
                "No credentials available. Attempting to authenticate..."
            )
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
            "per_page": 1000,  # Número de itens por página
            "page": 0,  # Página inicial
        }

        # For debugging
        self.logger.info(f"Starting API requests to {endpoint}")

        # Keep track of already processed items to avoid duplicates
        processed_items = set()

        try:
            # Make the first request outside the loop to check the structure
            response = requests.post(url, headers=self.credentials, json=request_params)
            response.raise_for_status()
            result = response.json()

            # Only handle positions data
            if key in result:
                data_key = key
                # Handle positions data from first request
                if result.get(data_key):
                    if isinstance(result[data_key], list):
                        items = result[data_key]
                    else:
                        items = list(result[data_key].values())

                    # if data_key == "objects":
                    #    items = items[0].get("instrument_positions", [])

                    # Add unique items only
                    added_count = self._add_unique_items(
                        all_data, items, processed_items
                    )
                    self.logger.info(
                        f"Added {added_count} unique records from positions page 0"
                    )

                # Continue with pagination
                page = 1

                # Continue pagination until we don't find new records
                empty_pages_count = 0
                max_empty_pages = 2  # Stop after 2 consecutive empty pages
                max_pages = 100  # Safety limit

                while page < max_pages:
                    # Update page number for next request
                    request_params["pagination"]["page"] = page

                    # Log current page
                    self.logger.info(f"Fetching {data_key} page {page}...")

                    # Make the request
                    response = requests.post(
                        url, headers=self.credentials, json=request_params
                    )
                    response.raise_for_status()
                    result = response.json()

                    # Check if we got any data for this key
                    if not result.get(data_key):
                        self.logger.info(
                            f"No {data_key} data in response for page {page}"
                        )
                        empty_pages_count += 1
                        if empty_pages_count >= max_empty_pages:
                            self.logger.info(
                                f"Stopping after {empty_pages_count} consecutive empty pages"
                            )
                            break
                        page += 1
                        continue

                    # Extract items based on data structure
                    if isinstance(result[data_key], list):
                        items = result[data_key]
                    else:
                        items = list(result[data_key].values())

                    # Check if we got empty data
                    if len(items) == 0:
                        self.logger.info(f"Empty data for {data_key} on page {page}")
                        empty_pages_count += 1
                        if empty_pages_count >= max_empty_pages:
                            self.logger.info(
                                f"Stopping after {empty_pages_count} consecutive empty pages"
                            )
                            break
                        page += 1
                        continue

                    # Reset empty pages counter since we got data
                    empty_pages_count = 0

                    # Add unique items to our result
                    added_count = self._add_unique_items(
                        all_data, items, processed_items
                    )
                    self.logger.info(
                        f"Added {added_count} unique records from {data_key} page {page} (filtered from {len(items)} total)"
                    )

                    # If we didn't add any new items, we've reached the end of unique data
                    if added_count == 0:
                        self.logger.info(
                            f"No new unique items on page {page}, stopping pagination"
                        )
                        break

                    # If we got fewer items than requested, we've reached the last page
                    if len(items) < request_params["pagination"]["per_page"]:
                        self.logger.info(
                            f"Received {len(items)} items, less than page size. Likely last page."
                        )
                        break

                    page += 1

            else:
                # Unknown response structure
                self.logger.warning(
                    f"Expected 'positions' key, but found: {list(result.keys())}"
                )
                return pd.DataFrame()

        except requests.exceptions.HTTPError as e:
            self.logger.error(f"HTTP error: {str(e)}")
            if e.response.status_code == 401:  # Unauthorized
                self.logger.info(
                    "Token may have expired. Attempting to reauthenticate..."
                )
                try:
                    self.authenticate()
                    # Try again with fresh credentials
                    return self.fetch_data(endpoint, params)
                except Exception as auth_error:
                    self.logger.error(f"Reauthentication failed: {str(auth_error)}")
            return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error fetching data from API: {str(e)}")
            return pd.DataFrame()

        # Return final data as DataFrame
        self.logger.info(f"Total unique records collected: {len(all_data)}")
        return pd.DataFrame(all_data) if all_data else pd.DataFrame()

    def _add_unique_items(self, all_data, items, processed_items):
        """
        Add only unique items to all_data and update processed_items set.
        Returns the count of newly added items.
        """
        added_count = 0

        for item in items:
            # Create a unique identifier for this item
            item_id = self._get_item_identifier(item)

            # Only add if we haven't seen this item before
            if item_id not in processed_items:
                processed_items.add(item_id)
                all_data.append(item)
                added_count += 1

        return added_count

    def _get_item_identifier(self, item):
        """
        Create a unique identifier for an item based on its key fields.
        This helps detect duplicates across pages.
        """
        # Build a list of key-value pairs for fields that uniquely identify a record
        key_parts = []

        # Use the most important fields for identification
        # Adjust these keys based on your specific data structure
        important_keys = ["portfolio_name", "date", "investor_names", "shares_amount"]

        for key in important_keys:
            if key in item:
                key_parts.append(f"{key}:{item[key]}")

        # If no important keys found, use a string representation of the sorted item
        if not key_parts:
            return str(sorted(item.items()))

        # Join all parts with a separator
        return "|".join(key_parts)
