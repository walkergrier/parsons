import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, cast
from urllib.parse import parse_qs, urlparse

import yaml

from parsons import Table
from parsons.utilities import check_env
from parsons.utilities.api_connector import APIConnector

logger = logging.getLogger(__name__)


class NationBuilderV1:
    """
    Instantiate the NationBuilder class

    `Args:`
        slug: str
            The Nation Builder slug Not required if ``NB_SLUG`` env variable set. The slug is the
            nation slug of the nation from which your application is requesting approval to retrieve
            data via the NationBuilder API. For example, your application's user could provide this
            slug via a text field in your application.
        access_token: str
            The Nation Builder access_token Not required if ``NB_ACCESS_TOKEN`` env variable set.
    """

    def __init__(self, slug: Optional[str] = None, access_token: Optional[str] = None) -> None:
        slug = check_env.check("NB_SLUG", slug)
        token = check_env.check("NB_ACCESS_TOKEN", access_token)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(NationBuilderV1.get_auth_headers(token))

        self.client = APIConnector(NationBuilderV1.get_uri(slug), headers=headers)

    @classmethod
    def get_uri(cls, slug: Optional[str]) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com/api/v1"

    @classmethod
    def get_auth_headers(cls, access_token: Optional[str]) -> Dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't None")

        if not isinstance(access_token, str):
            raise ValueError("access_token must be an str")

        if len(access_token.strip()) == 0:
            raise ValueError("access_token can't be an empty str")

        return {"authorization": f"Bearer {access_token}"}

    @classmethod
    def parse_next_params(cls, next_value: str) -> Tuple[str, str]:
        next_params = parse_qs(urlparse(next_value).query)

        if "__nonce" not in next_params:
            raise ValueError("__nonce param not found")

        if "__token" not in next_params:
            raise ValueError("__token param not found")

        nonce = next_params["__nonce"][0]
        token = next_params["__token"][0]

        return nonce, token

    @classmethod
    def make_next_url(cls, original_url: str, nonce: str, token: str) -> str:
        return f"{original_url}?limit=100&__nonce={nonce}&__token={token}"

    def get_people(self) -> Table:
        """
        `Returns:`
            A Table of all people stored in Nation Builder.
        """
        data = []
        original_url = "people"

        url = f"{original_url}"

        while True:
            try:
                logging.debug("sending request %s" % url)
                response = self.client.get_request(url)

                res = response.get("results", None)

                if res is None:
                    break

                logging.debug("response got %s records" % len(res))

                data.extend(res)

                if response.get("next", None):
                    nonce, token = NationBuilderV1.parse_next_params(response["next"])
                    url = NationBuilderV1.make_next_url(original_url, nonce, token)
                else:
                    break
            except Exception as error:
                logging.error("error requesting data from Nation Builder: %s" % error)

                wait_time = 30
                logging.info("waiting %d seconds before retrying" % wait_time)
                time.sleep(wait_time)

        return Table(data)

    def update_person(self, person_id: str, person: Dict[str, Any]) -> Dict[str, Any]:
        """
        This method updates a person with the provided id to have the provided data. It returns a
        full representation of the updated person.

        `Args:`
            person_id: str
                Nation Builder person id.
            data: dict
                Nation builder person object.
                For example {"email": "user@example.com", "tags": ["foo", "bar"]}
                Docs: https://nationbuilder.com/people_api
        `Returns:`
            A person object with the updated data.
        """
        if person_id is None:
            raise ValueError("person_id can't None")

        if not isinstance(person_id, str):
            raise ValueError("person_id must be a str")

        if len(person_id.strip()) == 0:
            raise ValueError("person_id can't be an empty str")

        if not isinstance(person, dict):
            raise ValueError("person must be a dict")

        url = f"people/{person_id}"
        response = self.client.put_request(url, data=json.dumps({"person": person}))
        response = cast("Dict[str, Any]", response)

        return response

    def upsert_person(self, person: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """
        Updates a matched person or creates a new one if the person doesn't exist.

        This method attempts to match the input person resource to a person already in the
        nation. If a match is found, the matched person is updated. If a match is not found, a new
        person is created. Matches are found by including one of the following IDs in the request:

            - civicrm_id
            - county_file_id
            - dw_id
            - external_id
            - email
            - facebook_username
            - ngp_id
            - salesforce_id
            - twitter_login
            - van_id

        `Args:`
            data: dict
                Nation builder person object.
                For example {"email": "user@example.com", "tags": ["foo", "bar"]}
                Docs: https://nationbuilder.com/people_api
        `Returns:`
            A tuple of `created` and `person` object with the updated data. If the request fails
            the method will return a tuple of `False` and `None`.
        """

        _required_keys = [
            "civicrm_id",
            "county_file_id",
            "dw_id",
            "external_id",
            "email",
            "facebook_username",
            "ngp_id",
            "salesforce_id",
            "twitter_login",
            "van_id",
        ]

        if not isinstance(person, dict):
            raise ValueError("person must be a dict")

        has_required_key = any(x in person for x in _required_keys)

        if not has_required_key:
            _keys = ", ".join(_required_keys)
            raise ValueError(f"person dict must contain at least one key of {_keys}")

        url = "people/push"
        response = self.client.request(url, "PUT", data=json.dumps({"person": person}))

        self.client.validate_response(response)

        if response.status_code == 200:
            if self.client.json_check(response):
                return (False, response.json())

        if response.status_code == 201:
            if self.client.json_check(response):
                return (True, response.json())

        return (False, None)


class DynamicMethodCreator(type):
    """
    A metaclass that dynamically adds methods to a class based on its 'method_configs'.
    Each configuration dictionary defines a method, where its keys and values
    become keyword arguments and their default values for the dynamically created method.
    """

    def __new__(mcs, name, bases, namespace):
        # Retrieve the list of method configurations from the class's namespace

        def read_spec():
            with open(Path(__file__).parent / r"openapi-spec.yaml", "r") as f:
                return yaml.safe_load(f)

        def camel_to_snake(name):
            # Insert an underscore before any uppercase letter that is not at the beginning of the string
            # and convert the entire string to lowercase.
            s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
            return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

        def search_spec(parameters, spec):
            def _search_spec(struct, i):
                var = i.pop(1)
                if len(i) != 1:
                    _search_spec(struct[var], i)
                return struct[var]

            if "$ref" in parameters:
                return _search_spec(spec, parameters["$ref"].split(r"/"))
            return parameters

        def get_method_configs():
            api_spec = read_spec()

            method_configs = (
                {
                    "operation_id": camel_to_snake(api_spec["paths"][path][method]["operationId"]),
                    "summary": api_spec["paths"][path][method]["summary"],
                    "method": method,
                    "path": path,
                    "parameters": (
                        [
                            search_spec(p, api_spec)
                            for p in api_spec["paths"][path][method]["parameters"]
                        ]
                        if "parameters" in api_spec["paths"][path][method]
                        else []
                    )
                    + (
                        [search_spec(p, api_spec) for p in api_spec["paths"][path]["parameters"]]
                        if "parameters" in api_spec["paths"][path].keys()
                        else []
                    ),
                    "request_schema": (
                        search_spec(
                            api_spec["paths"][path][method]["requestBody"]["content"][
                                r"application/json"
                            ]["schema"],
                            api_spec,
                        )
                        if "requestBody" in api_spec["paths"][path][method]
                        else None
                    ),
                }
                for path in api_spec["paths"]
                for method in api_spec["paths"][path]
                if method != "parameters"
            )
            return method_configs

        method_configs = get_method_configs()

        # Iterate over each dictionary in the method_configs list
        for config_dict in method_configs:
            # Extract the name of the method to be created using "operation_id"
            method_name = config_dict.get("operation_id")
            if not method_name:
                # Skip this configuration if no method name is provided
                continue

            # Extract default arguments for the new method from the config_dict.
            # We exclude 'operation_id' as it's the method's name, not an argument.
            method_defaults = {k: v for k, v in config_dict.items() if k != "operation_id"}

            # Define the function that will become the new method.
            # This function will accept arbitrary keyword arguments (**runtime_kwargs),
            # which will override the defaults defined in method_defaults.
            def _method_template(self, **runtime_kwargs):
                """
                A dynamically created method that combines predefined defaults
                with runtime arguments and calls the resource method.

                Args:
                    **runtime_kwargs: Any keyword arguments passed when calling this method,
                                        which will override the defaults.
                """
                # Combine the pre-defined defaults with any runtime arguments.
                # Runtime arguments take precedence, effectively overriding defaults.
                final_args = {**method_defaults, **runtime_kwargs}

<<<<<<< HEAD
=======
                # Extract 'count' and 'message' from the final combined arguments.
                # Provide sensible fallbacks if 'count' or 'message' are not present
                # in either the defaults or the runtime arguments.

>>>>>>> 1ef8944749178fe138d0d99c26356331b69498ab
                self.resource(
                    req_type=final_args.get("req_type"),
                    url_path=final_args.get("url_path"),
                )

            # Assign a unique name to the dynamically created function.
            # This is good practice for introspection (e.g., when debugging).
            _method_template.__name__ = method_name

            # Add the dynamically created function to the class's namespace.
            # This makes it a callable method of the class being created.
            namespace[method_name] = _method_template

        # Call the superclass's __new__ method to finalize the class creation process.
        return super().__new__(mcs, name, bases, namespace)


class NationBuilderV2(metaclass=DynamicMethodCreator):
    def __init__(
        self,
        slug: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
    ) -> None:
        slug = check_env.check("NB_SLUG", slug)
        token = check_env.check("NB_ACCESS_TOKEN", access_token)
        refresh_token = check_env.check("NB_REFRESH_TOKEN", refresh_token)

        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        headers.update(NationBuilderV2.get_auth_headers(token))

        self.client = APIConnector(NationBuilderV2.get_uri(slug), headers=headers)
<<<<<<< HEAD
=======
        # List of dictionaries defining the dynamic methods.
        # Each dictionary specifies the method 'operation_id' and other key-value pairs
        # that become default keyword arguments for that method.
>>>>>>> 1ef8944749178fe138d0d99c26356331b69498ab

    @classmethod
    def get_uri(cls, slug: Optional[str]) -> str:
        if slug is None:
            raise ValueError("slug can't None")

        if not isinstance(slug, str):
            raise ValueError("slug must be an str")

        if len(slug.strip()) == 0:
            raise ValueError("slug can't be an empty str")

        return f"https://{slug}.nationbuilder.com"

    @classmethod
    def get_auth_headers(cls, access_token: Optional[str]) -> Dict[str, str]:
        if access_token is None:
            raise ValueError("access_token can't None")

        if not isinstance(access_token, str):
            raise ValueError("access_token must be an str")

        if len(access_token.strip()) == 0:
            raise ValueError("access_token can't be an empty str")

        return {"authorization": f"Bearer {access_token}"}

    @classmethod
    def camel_to_snake(cls, name):
        # Insert an underscore before any uppercase letter that is not at the beginning of the string
        # and convert the entire string to lowercase.
        name = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
        name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
        return re.sub(r"(\s*_)", r"_", name).lower()

    def search_dict(self, parameters):
        def _search_dict(d, i):
            k = i.pop(1)
            return d[k] if len(i) == 1 else _search_dict(d[k], i)

        return (
            _search_dict(self.oa_spec, parameters["$ref"].split(r"/"))
            if "$ref" in parameters
            else parameters
        )

    # List of dictionaries defining the dynamic methods.
    # Each dictionary specifies the method 'operation_id' and other key-value pairs
    # that become default keyword arguments for that method.
    with open(Path(__file__).parent / r"openapi-spec.yaml", "r") as f:
        openapi_spec = yaml.safe_load(f)

        method_configs = chain.from_iterable(
            [
                [
                    {
                        "operation_id": NationBuilderV1.camel_to_snake(
                            openapi_spec["paths"][path][j]["operationId"]
                        ),
                        "method": j,
                        "path": path,
                    }
                    for j in openapi_spec["paths"][path]
                    if j != "parameters"
                ]
                for path in openapi_spec["paths"]
            ]
        )

    @classmethod
    def _to_table(cls, resp) -> Table:
        return Table(
            [
                {"id": i["id"], "type": i["type"]}.update(i["attributes"])
                for i in resp.json()["data"]
            ],
        )

    def _get_next(self, resp):
<<<<<<< HEAD
        q = urlparse(resp.json()["links"]["next"])
        resp = self.client.get_request(q.path, params=q.query)
        return resp
=======
        if "next" in resp.json()["links"]:
            q = urlparse(resp.json()["links"]["next"])
            resp = self.client.get_request(q.path, params=q.query)
            return resp
>>>>>>> 1ef8944749178fe138d0d99c26356331b69498ab

    @classmethod
    def _get_all(self, resp: int, limit: int) -> Table:
        data = NationBuilderV2._to_table(resp)
        while limit != 0 and len(data) < limit:
            resp = self.get_next(resp)
            if resp is None:
                break
            data.stack(NationBuilderV2._to_table(resp))
        return data

    @classmethod
    def _param_builder(cls, param_name: str, param_dict: dict) -> dict:
        """Convert param dictionary into NationBuilder's param format."""
        if not param_dict:
            return {}

        params = {}
        for key, value in param_dict.items():
            if isinstance(value, dict):  # Handling complex cases
                params.update(
                    {f"{param_name}[{key}][{operator}]": val for operator, val in value.items()}
                )
            else:  # Simple case
                params[f"{param_name}[{key}]"] = value
        return params

<<<<<<< HEAD
    def _field_params(self, resource: str, fields: str | list) -> dict:
=======
    @classmethod
    def _field_params(cls, resource: str, fields: str | list) -> dict:
>>>>>>> 1ef8944749178fe138d0d99c26356331b69498ab
        if not fields:
            return {}
        elif isinstance(fields, str):
            return {f"fields[{resource}]": fields}
        elif isinstance(fields, list):
            return {f"fields[{resource}]": ",".join(fields)}
        else:
            raise TypeError("fields should be str or list")

    def resource(
        self,
        data: dict = None,
        id = None,
        client = None,
        req_type: str = None,
        url: str = None,
        resource: str = None,
        filters: dict = None,
        fields: list = None,
        sort_by: dict = None,
        count_results: bool = False,
        page_size: int = 100,
        all_pages: bool = False,
        results_limit: int = 0,
    ) -> Table:
        """
        Generic function to fetch data from any NationBuilder v2 API endpoint.
        :param resource: API resource (e.g., "people", "donations", "events").
        :param filters: Dictionary of filters.
        :param per_page: Number of records per page.
        :param all_pages: Whether to fetch all pages.
        """

        params = self._param_builder("filter", filters)
        params.update(self._field_params(resource=resource, fields=fields))
        if sort_by:
            params["sort"] = sort_by
        if count_results or all_pages:
            params["stats[total]"] = "count"
        params["page_size"] = min(100, max(1, page_size))

        valid_req_params = {
            "GET": ("url", "params", "return_format"),
            "POST": ("url", "params", "data", "json", "success_codes"),
            "DELETE": ("url", "params", "success_codes"),
            "PUT": ("url", "params", "data", "json", "success_codes"),
            "PATCH": ("url", "params", "data", "json", "success_codes"),
        }

        req_params = {
            "url": url,
            "params": params,
            "data": data,
            "json": json_data,
            "return_format": return_format,
            "success_codes": succss_code,
        }

        resp = client(
            **{
                k: v
                for k, v in req_params.items()
                if v is not None and k in valid_req_params[req_type]
            }
        )

        if all_pages and isinstance(client, self.client.get_request):
            return self._get_all(resp, results_limit)
        return NationBuilder._to_table(resp)


class NationBuilder:
    def __new__(
        cls,
        slug: Optional[str] = None,
        access_token: Optional[str] = None,
        refresh_token: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
        parsons_version: str = "v1",
    ):
        parsons_version = check_env.check("NB_PARSONS_VERSION", parsons_version)
        if parsons_version == "v1":
            logger.info("Consider upgrading to version 2 of the NationBuilder connector!")
            logger.info(
                "See docs for more information: https://move-coop.github.io/parsons/html/latest/nation_builder.html"
            )
            return NationBuilderV1(slug=slug, access_token=client_id, client_secret=client_secret)
        if parsons_version == "v2":
            return NationBuilderV2(
                slug=slug,
                access_token=access_token,
                refresh_token=refresh_token,
                client_id=client_id,
                client_secret=client_secret,
                redirect_uri=redirect_uri,
            )
        raise ValueError(f"{parsons_version} not supported")
