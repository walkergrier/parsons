from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Callable
    from parsons import Table
    from parsons.utilities.api_connector import APIConnector

class SignupsNB:
    client: APIConnector
    _list_resource: Callable
    _post_resource: Callable
    _show_resource: Callable
    _patch_resource: Callable
    _delete_resource: Callable

    def fetch_signups(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
        return self._list_resource(
            resource="signups",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_signup(self, payload: dict, params: dict) -> dict:
        return self._post_resource(resource="signups", params=params, payload=payload)

    def push_signup(self, payload: dict, params: dict | None = None) -> dict:
        params = params if params else {}
        required_keys: list[str] = [
            "civicrm_id",
            "county_file_id",
            "datatrust_id",
            "dw_id",
            "email",
            "external_id",
            "mobile_number",
            "ngp_id",
            "pf_strat_id",
            "phone_number",
            "rnc_id",
            "rnc_regid",
            "salesforce_id",
            "state_file_id",
            "van_id",
            "work_phone_number",
        ]
        has_required_key: bool = any(x in payload for x in required_keys)
        if not has_required_key:
            keys: str = ", ".join(required_keys)
            raise ValueError(f"payload dict must contain at least one key of {keys}")
        return self._upsert_resource(resource="signups", payload=payload, params=params)

    def upsert_signup(self, **kwargs):
        return self.push_signup(**kwargs)

    def update_signup(self, id: int | str, payload: dict | None = None, params: dict | None = None):
        return self._patch_resource(resource="signups", id=id, params=params, payload=payload)

    def show_signup(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        extra_fields: list[str] | str | bool = True,
        **kwargs,
    ) -> dict:
        if extra_fields is True:
            extra_fields = [
                "billing_address",
                "mailing_address",
                "home_address",
                "primary_address",
                "registered_address",
                "user_submitted_address",
                "work_address",
                "profile_image_url",
            ]
        elif extra_fields is False:
            extra_fields = []
        elif isinstance(extra_fields, str):
            extra_fields = [extra_fields]

        return self._show_resource(
            resource="signups", id=id, params=params, sideload=sideload, **kwargs
        )

    def show_me(self, params: dict | None = None):
        # id does nothing
        return self._show_resource(id=0, resource="signups", params=params, url="signups/me")