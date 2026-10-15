from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Callable
    from parsons import Table
    from parsons.utilities.api_connector import APIConnector

class ListsNB:
    client: APIConnector
    _list_resource: Callable
    _post_resource: Callable
    _show_resource: Callable
    _patch_resource: Callable
    _delete_resource: Callable


    def fetch_lists(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
        return self._list_resource(
            resource="lists", filters=filters, params=params, all_results=all_results, **kwargs
        )

    def show_list(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ) -> dict:
        return self._show_resource(
            resource="lists", id=id, params=params, sideload=sideload, **kwargs
        )
        
    def post_list(self, payload: dict, params: dict | None = None):
        return self._post_resource(resource="lists", params=None, payload=payload)

    def add_signups_to_list(
        self,
        list_id: int | str,
        signup_ids: list[str | int] | str | int,
        params: dict | None = None,
        **kwargs,
    ):
        signup_ids = signup_ids if isinstance(signup_ids, list) else [signup_ids]
        payload: dict[str, dict] = {
            "data": {"id": list_id, "type": "lists", "signup_ids": signup_ids}
        }
        return self.client.patch_request(
            url=f"lists/{id}/add_signups",
            params=params,
            json=payload,
        )

    def remove_signups_from_list(
        self,
        list_id: int | str,
        signup_ids: list[str | int] | str | int,
        params: dict | None = None,
        **kwargs,
    ):
        signup_ids = signup_ids if isinstance(signup_ids, list) else [signup_ids]
        payload: dict[str, dict] = {
            "data": {"id": list_id, "type": "lists", "signup_ids": signup_ids}
        }
        return self.client.patch_request(
            url=f"lists/{id}/remove_signups",
            params=params,
            json=payload,
        )

    # def fetch_signups_on_list(
    #     self, id: int | str, params: dict | None = None, all_results: bool = True, **kwargs
    # ) -> Table:
    #     return self._list_resource(
    #         resource="signups",
    #         params=params,
    #         all_results=all_results,
    #         url=f"lists/{id}/signups",
    #         **kwargs,
    #     )