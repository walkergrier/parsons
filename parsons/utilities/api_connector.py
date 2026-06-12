import logging
import urllib.parse

from requests import Response, Session
from requests import request as _request
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError
from simplejson.errors import JSONDecodeError
from urllib3.util.retry import Retry

from parsons import Table
from parsons.utilities import check_env

logger: logging.Logger = logging.getLogger(name=__name__)


class APIConnector:
    """
    The API Connector is a low level class for API requests that other connectors
    can utilize. It is understood that there are many standards for REST APIs and it will be
    difficult to create a universal connector. The goal of this class is create series
    of utilities that can be mixed and matched po, hopefully, meet the needs of the specific
    API.

    `Args:`
        uri: str
            The base uri for the api. Must include a trailing '/' (e.g. ``http://myapi.com/v1/``)
        headers: dict | None
            The request headers
        auth: dict
            The request authorization parameters
        pagination_key: str | None
            The name of the key in the response json where the pagination url is
            located. Required for pagination.
        data_key: str | None
            The name of the key in the response json where the data is contained. Required
            if the data is nested in the response json
        as_session: bool | None
            Whether to use a persistent `requests.Session` for API calls. If `True`,
            enables retries and sets default headers/auth on the session. If `None`,
            checks the `PARSONS_AS_SESSION` environment variable. Defaults to `False`.
        default_timeout: int | None
            The default timeout in seconds for all requests when`as_session` is `True` and
            `timeout` passed to a request is not `None`.
    `Returns`:
        APIConnector class
    """

    def __init__(
        self,
        uri: str,
        headers: dict | None = None,
        auth=None,
        pagination_key: str | None = None,
        data_key: str | None = None,
        as_session: bool | None = None,
        default_timeout: int | None = 20,
    ) -> None:
        # Add a trailing slash if its missing
        if not uri.endswith("/"):
            uri = uri + "/"

        # If `as_session` is `None`, check for env variable,
        # If `as_session` is `Truthy` or `Falsy` it overrides env variable
        if as_session is None:
            self.as_session: bool = bool(check_env.check("PARSONS_AS_SESSION", as_session, True))
        else:
            self.as_session: bool = as_session

        self.uri: str = uri
        self.pagination_key: str | None = pagination_key
        self.data_key: str | None = data_key

        if self.as_session:
            retries = Retry(
                total=3, backoff_factor=1, allowed_methods={"HEAD", "GET", "PUT", "DELETE", "PATCH"}
            )
            adapter = HTTPAdapter(max_retries=retries, pool_connections=30, pool_maxsize=30)

            self.session = Session()
            self.session.mount(prefix="https://", adapter=adapter)
            self.session.mount(prefix="http://", adapter=adapter)

            self.default_timeout: int | None = default_timeout

            if headers:
                self.session.headers.update(headers)
            if auth:
                self.session.auth = auth
        else:
            self.headers: dict | None = headers
            self.auth = auth

    def request(
        self,
        url: str,
        req_type: str,
        json: dict | None = None,
        data: str | dict | bytes | None = None,
        params: dict | None = None,
        timeout: int | None = None,
    ) -> Response:
        """
        Base request using requests libary.

        `Args:`
            url: str
                The url request string; if ``url`` is a relative URL, it will be joined with
                the ``uri`` of the ``APIConnector``; if ``url`` is an absolute URL, it will
                be used as is.
            req_type: str
                The request type. One of GET, POST, PATCH, DELETE, OPTIONS
            json: dict | None
                The payload of the request object. By using json, it will automatically
                serialize the dictionary
            data: str | dict | bytes | None
                The payload of the request object. Use instead of json in some instances.
            params: dict | None
                The parameters to append to the url (e.g. http://myapi.com/things?id=1)
            timeout: int | None
                The timeout in seconds for this specific request. If provided, this value
                overrides any class-level `default_timeout`. Defaults to `None` (no timeout).
        `Returns:`
            requests response
        """
        full_url: str = urllib.parse.urljoin(base=self.uri, url=url)

        if self.as_session:
            req_timeout: int | None = timeout if timeout is not None else self.default_timeout
            return self.session.request(
                method=req_type,
                url=full_url,
                json=json,
                data=data,
                params=params,
                timeout=req_timeout,
            )
        return _request(
            method=req_type,
            url=full_url,
            headers=self.headers,
            auth=self.auth,
            json=json,
            data=data,
            params=params,
            timeout=timeout,  #! Since we've added a `timeout` parameter, it can be used here
        )

    def get_request(self, url: str, params: dict | None = None, return_format: str = "json"):
        """
        Make a GET request.

        Args:
            url: str
                A complete and valid url for the api request
            params: dict | None
                The request parameters
            return_format: str

        Returns:
                A requests response object
        """
        r: Response = self.request(url=url, req_type="GET", params=params)

        self.validate_response(resp=r)

        if return_format == "json":
            logger.debug(msg=r.json())
            return r.json()
        elif return_format == "content":
            return r.content
        else:
            raise RuntimeError(f"{return_format} is not a valid format, change to json or content")

    def post_request(
        self,
        url: str,
        params: dict | None = None,
        data: str | dict | bytes | None = None,
        json: dict | None = None,
        success_codes: int | list[int] | None = None,
    ) -> dict | int | None:
        """
        Make a POST request.

        `Args:`
            url: str
                A complete and valid url for the api request
            params: dict | None
                The request parameters
            data: str | dict | bytes | None
                A data object to post
            json: dict | None
                A JSON object to post
            success_codes: int | list[int] | None
                The expected success codes to be returned. If not provided, accepts 200, 201, 202, and 204.
        `Returns:`
            A requests response object
        """
        if success_codes is None:
            success_codes = [200, 201, 202, 204]
        elif isinstance(success_codes, int):
            success_codes = [success_codes]

        r: Response = self.request(url=url, req_type="POST", params=params, data=data, json=json)

        # Validate the response and lift up an errors.
        self.validate_response(resp=r)

        # Check for a valid success code for the POST. Some APIs return messages with the
        # success code and some do not. Be able to account for both of these types.
        if r.status_code in success_codes:
            if self.json_check(resp=r):
                return r.json()
            else:
                return r.status_code

    def delete_request(
        self, url: str, params: dict | None = None, success_codes: int | list[int] | None = None
    ) -> dict | int | None:
        """
        Make a DELETE request.

        Args:
            url: str
                A complete and valid url for the api request
            params: dict | None
                The request parameters
            success_codes: int | list[int] | None
                The expected success codes to be returned. If not provided, accepts 200, 201, 204.
        Returns:
                A requests response object or status code
        """
        if success_codes is None:
            success_codes = [200, 201, 204]
        elif isinstance(success_codes, int):
            success_codes = [success_codes]

        r: Response = self.request(url=url, req_type="DELETE", params=params)

        self.validate_response(resp=r)

        # Check for a valid success code for the POST. Some APIs return messages with the
        # success code and some do not. Be able to account for both of these types.
        if r.status_code in success_codes:
            if self.json_check(resp=r):
                return r.json()
            else:
                return r.status_code

    def put_request(
        self,
        url: str,
        data: str | dict | bytes | None = None,
        json: dict | None = None,
        params: dict | None = None,
        success_codes: int | list[int] | None = None,
    ) -> dict | int | None:
        """
        Make a PUT request.

        Args:
            url: str
                A complete and valid url for the api request
            data: str | dict | bytes | None
                A data object to post
            json: dict | None
                A JSON object to post
            params: dict | None
                The request parameters
            success_codes: int | list[int] | None
                The expected success codes to be returned. If not provided, accepts 200, 201, 204.
        Returns:
                A requests response object
        """
        if success_codes is None:
            success_codes = [200, 201, 204]
        elif isinstance(success_codes, int):
            success_codes = [success_codes]

        r: Response = self.request(url=url, req_type="PUT", params=params, data=data, json=json)

        self.validate_response(resp=r)

        if r.status_code in success_codes:
            if self.json_check(resp=r):
                return r.json()
            else:
                return r.status_code

    def patch_request(
        self,
        url: str,
        params: dict | None = None,
        data: str | dict | bytes | None = None,
        json: dict | None = None,
        success_codes: int | list[int] | None = None,
    ) -> dict | int | None:
        """
        Make a PATCH request.

        `Args:`
            url: str
                A complete and valid url for the api request
            params: dict | None
                The request parameters
            data: str | dict | bytes | None
                A data object to post
            json: dict | None
                A JSON object to post
            success_codes: int | list[int] | None
                The expected success codes to be returned. If not provided, accepts 200, 201, and 204.
        `Returns:`
            A requests response object
        """
        if success_codes is None:
            success_codes = [200, 201, 204]
        elif isinstance(success_codes, int):
            success_codes = [success_codes]

        r: Response = self.request(url=url, req_type="PATCH", params=params, data=data, json=json)

        self.validate_response(resp=r)

        # Check for a valid success code for the POST. Some APIs return messages with the
        # success code and some do not. Be able to account for both of these types.
        if r.status_code in success_codes:
            if self.json_check(resp=r):
                return r.json()
            else:
                return r.status_code

    def validate_response(self, resp: Response) -> None:
        """
        Validate that the response is not an error code. If it is, then raise an error
        and display the error message.

        `Args:`
            resp: Response
                A response object
        """
        if resp.status_code >= 400:
            if resp.reason:
                message = f"HTTP error occurred ({resp.status_code}): {resp.reason}"
            elif resp.text:
                message = f"HTTP error occurred ({resp.status_code}): {resp.text}"
            else:
                message: str = f"HTTP error occurred ({resp.status_code})"

            # Some errors return JSONs with useful info about the error. Return it if exists.
            if self.json_check(resp=resp):
                raise HTTPError(f"{message}, json: {resp.json()}")
            else:
                raise HTTPError(message)

    def data_parse(self, resp: dict | list) -> dict | list:
        """
        Determines if the response json has nested data. If it is nested, it just returns the
        data. This is useful in dealing with requests that might return multiple records, while
        others might return only a single record.

        `Args:`
            resp:
                A response dictionary
        `Returns:`
            dict | list
                A dictionary of data.
        """

        # TODO: Some response jsons are enclosed in a list. Need to deal with unpacking and/or
        # not assuming that it is going to be a dict.

        # In some instances responses are just lists.
        if isinstance(resp, list):
            return resp

        if self.data_key and self.data_key in resp:
            return resp[self.data_key]
        else:
            return resp

    # There are many different ways in which APIs indicate whether there is a next page
    # of data following the initial request. The goal is build out a series of utilities
    # that mean most of the most common use cases.

    def next_page_check_url(self, resp: dict) -> bool | None:
        """
        Check to determine if there is a next page. This requires that the response json
        contains a pagination key that is empty if there is not a next page.

        `Args:`
            resp:
                A response dictionary
        `Returns:
            boolean
        """
        if self.pagination_key and self.pagination_key in resp:
            if resp[self.pagination_key]:
                return True
        else:
            return False

    def json_check(self, resp: Response) -> bool:
        """
        Check to see if a response has a json included in it.
        """
        try:
            resp.json()
            return True
        except JSONDecodeError:
            return False

    def convert_to_table(self, data) -> Table | None:
        """Internal method to create a Parsons table from a data element."""
        table = None
        table = Table(data) if type(data) is list else Table([data])

        return table
