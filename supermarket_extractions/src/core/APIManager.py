import requests, time, logging


class APIManager:
    def __init__(self, config_json, logger=logging.getLogger(__name__),max_retries=3, retry_delay=10, rate_limit=20, timeout = 60):
        self.config = config_json
        self.session = requests.Session()
        self.logger = logger
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.rate_limit = rate_limit
        self.last_request_time = 0
        self.circuit_open = False
        self.circuit_open_since = 0
        self.circuit_reset_timeout = timeout  # 1 minute to reset the circuit breaker
        
    def request(self, endpoint_key, url_params={}, aditional_params={}, aditional_headers={}, json_data = None, attempt=1, error_expected=False):
        """
        Sends a request to the specified endpoint. Follows the configuration file pattern from the config folder (example: market_001.json).
        """
        config_endpoint = self.config['endpoints'][endpoint_key]
        
        url = self.config['base_url'] + config_endpoint['path'].format(**url_params)

        params = config_endpoint['parameters']
        
        for add_param in aditional_params:
            params[add_param] = aditional_params[add_param]

        header = self.config['default_headers']

        for add_header in aditional_headers:
            header[add_header] = aditional_headers[add_header]

        self.session.params = params
        self.session.headers = header

        method = config_endpoint['method']

    # Check rate limit and delay if necessary
        self._check_rate_limit()

    # Stop if timeout is exceeded
        self._handle_circuit_breaker()

        try:
            response = self.session.request(
                method=method,
                url=url,
                json=json_data
            )
            if not error_expected:
                response.raise_for_status()
                return response

            if error_expected:
                return response
            
        except requests.exceptions.RequestException as e:
            if attempt < self.max_retries:
                delay = self.retry_delay * (attempt)
                self.logger.warning(f"Error on request, try number {attempt}/{self.max_retries}. "
                             f"Error: {str(e)}. Trying again in {delay}s...")
                time.sleep(delay)
                return self.request(endpoint_key, url_params, aditional_params, aditional_headers, json_data, attempt + 1)
            else:
                self.circuit_open = True
                self.circuit_open_since = time.time()
                self.logger.warning(f"Error on request, try number  {attempt}/{self.max_retries}. "
                             f"Error: {str(e)}. Circuit Breaker active - service unavailable")
                raise

    
    def _check_rate_limit(self):
        """
        Implements the Rate Limit pattern (maximum requests per second).
        """
        if not self.rate_limit:
            return

        current_time = time.time()
        elapsed = current_time - self.last_request_time
        min_delay = 1 / self.rate_limit

        if elapsed < min_delay:
            delay = min_delay - elapsed
            time.sleep(delay)

        self.last_request_time = time.time()
    
    def _handle_circuit_breaker(self):
        """
        Implements the Circuit Breaker pattern.
        """
        if self.circuit_open:
            if time.time() - self.circuit_open_since > self.circuit_reset_timeout:
                self.circuit_open = False
                self.logger.warning("Circuit Breaker: Trying to restart the connection...")
            else:
                raise Exception("Circuit Breaker ativo - service unavailable")

    def request_raw(self, endpoint_key, url_params={}, aditional_params={}, aditional_headers={}, json_data = None, attempt=1):
        """
        Sends a request to the specified endpoint. Follows the configuration file pattern from the config folder (example: market_001.json).
        """
        config_endpoint = self.config['endpoints'][endpoint_key]
        
        url = self.config['base_url'] + config_endpoint['path'].format(**url_params)

        params = config_endpoint['parameters']
        
        for add_param in aditional_params:
            params[add_param] = aditional_params[add_param]

        header = self.config['default_headers']

        for add_header in aditional_headers:
            header[add_header] = aditional_headers[add_header]

        self.session.params = params
        self.session.headers = header

        method = config_endpoint['method']

        response = self.session.request(
                method=method,
                url=url,
                json=json_data
            )
        return response

