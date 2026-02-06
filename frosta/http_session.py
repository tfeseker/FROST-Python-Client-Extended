"""
HTTP session optimization for FROST client.

Provides connection pooling and reuse to reduce overhead of establishing
new connections for each request to the FROST server.
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import logging

logger = logging.getLogger(__name__)


class FrostHTTPSession:
    """Manages HTTP session with connection pooling for FROST API calls."""
    
    def __init__(self, pool_connections=10, pool_maxsize=20, max_retries=3):
        """
        Initialize HTTP session with connection pooling.
        
        Args:
            pool_connections: Number of connection pools to cache
            pool_maxsize: Maximum number of connections to save in the pool
            max_retries: Maximum number of retries for failed requests
        """
        self.session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        
        # Configure adapter with connection pooling
        adapter = HTTPAdapter(
            pool_connections=pool_connections,
            pool_maxsize=pool_maxsize,
            max_retries=retry_strategy
        )
        
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        logger.debug(f"HTTP session initialized: pool_connections={pool_connections}, "
                    f"pool_maxsize={pool_maxsize}, max_retries={max_retries}")
    
    def get(self, url, **kwargs):
        """Execute GET request using pooled connection."""
        return self.session.get(url, **kwargs)
    
    def post(self, url, **kwargs):
        """Execute POST request using pooled connection."""
        return self.session.post(url, **kwargs)
    
    def patch(self, url, **kwargs):
        """Execute PATCH request using pooled connection."""
        return self.session.patch(url, **kwargs)
    
    def put(self, url, **kwargs):
        """Execute PUT request using pooled connection."""
        return self.session.put(url, **kwargs)
    
    def delete(self, url, **kwargs):
        """Execute DELETE request using pooled connection."""
        return self.session.delete(url, **kwargs)
    
    def request(self, method, url, **kwargs):
        """Execute arbitrary HTTP request using pooled connection."""
        return self.session.request(method, url, **kwargs)
    
    def close(self):
        """Close the session and clean up connections."""
        self.session.close()
        logger.debug("HTTP session closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def patch_frost_service_with_session(frost_service, session=None):
    """
    Monkey-patch a frost_sta_client.SensorThingsService to use a shared HTTP session.
    
    This significantly improves performance by reusing connections instead of
    creating new ones for each request.
    
    Args:
        frost_service: Instance of frost_sta_client.service.SensorThingsService
        session: FrostHTTPSession instance, or None to create a new one
    
    Returns:
        The session instance being used (for cleanup later)
    """
    if session is None:
        session = FrostHTTPSession()
    
    # Store original execute method
    original_execute = frost_service.execute
    
    # Create wrapper that uses session
    def execute_with_session(method, url, **kwargs):
        if frost_service.auth_handler is not None:
            return session.request(
                method, 
                url, 
                proxies=frost_service.proxies, 
                auth=frost_service.auth_handler.add_auth_header(), 
                **kwargs
            )
        else:
            return session.request(
                method, 
                url, 
                proxies=frost_service.proxies, 
                **kwargs
            )
    
    # Replace execute method
    frost_service.execute = execute_with_session
    frost_service._http_session = session  # Store reference for cleanup
    
    logger.info("Patched FROST service to use HTTP session pooling")
    return session
