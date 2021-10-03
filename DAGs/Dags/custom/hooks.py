import datime as dt
import requests
from airflow.hooks.base_hook import BaseHook

class MovielensHook(BaseHook):
    """
    Hook for the Movie Lens API
    
    abstracts details of the Movielens (REST) API and provides several convenience methods for fetching data
    from the API. Also provides support for automatic retries of fauler requests, transparent handling of
    pagination, authentication, etc
    
    Parameters
    ----------
    conn_id : str
        ID of the connection to use to connect to the Movielens API. Connection is expected to include
        authentication details (login/password) and the host that is serving the API
    """
    DEFAULT_SCHEMA = "http"
    DEFAULT_PORT = 5000

    def __init__(self, conn_id, retry=3) -> None:
        super().__init__()
        self.conn_id =conn_id
        self.retry = retry

        self._session = None
        self._base_url = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def get_conn(self):
        """
        Return the connection used by the hook for query data.
        Should in principle not be used directly"""
        if self._session is None:
            # Fetch config for the given connection (host, login, etc).
            config = self.get_connection(self.conn_id)

            if not config.host:
                raise ValueError(f"No host specified in connection {self.__conn_id}")

                schema = config.schema or self.DEFAULT_SCHEMA
                port = config.port or self.DEFAULT_PORT

                self._base_url = f"{schema}://{config.host}:{port}"

                #Build our session instance, which we will use for any
                #Request to API
                self._session = request.Session()

            if config.login:
                self._session.auth = (config.login, config.password)
            
        return self._session, self._base_url

    def close(self):
        """closes any active session"""
        if self._session:
            self._session.close()
        self._session = None
        self._base_url = None
    def get_ratings(self, start_date=None, end_date=None, batch_size=100):
        """Fetches rating between the given start/end date
        Parameters
        ----------
        start_date : str
            Start date to start fetching ratings from (inclusive). Expected format is
             YYYY--MM--DD (equal to Airflow's ds format
        end_date: str
        batch_size: int
            Size of the batches (pages) to fetch from the API. Larger values mean less requests, but more
            data transferred per request"""

        yield from self._get_with_pagination(
            endpoint="/ratings",
            params={"start_date": start_date, "end_date": end_date},
            batch_size=batch_size,
        )    
    def get_ratings_for_month(self, year, month, batch_size=100):
        start_date = dt.datetime(year, month, day=1)
        if month == 12:
            end_date = dt.datetime(year+1, month, day=1)
        else:
            end_date = dt.datetime(year, month, day=1)
        
        yield from self.get_ratings(
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d"),
            batch_size=batch_size,
        )

    def _get_with_pagination(self, endpoint, params, batch_size=100):
        """fetches records using a get request with given url/params,
        taking pagination into account"""
        session, base_url = self.get_conn()
        url = base_url + endpoint

        offset = 0
        total = None
        while total is None or offset < total:
            response = session.get(
                url, params={**params, **{"offset": offset, "limit": batch_size}}
            )
            response.raise_for_status()
            response_json = response.json()

            yield from response_json["result"]

            offset += batch_size
            total = response_json["total"]