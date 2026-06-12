from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Generator, Callable
    from parsons import Table
    from parsons.utilities.api_connector import APIConnector

class EventsNB:
    _list_resource: Callable
    _post_resource: Callable
    _show_resource: Callable
    _patch_resource: Callable
    _delete_resource: Callable


    def fetch_events(
        self,
        filters: dict | None = None,
        params: dict | None = None,
        all_results: bool = False,
        **kwargs,
    ) -> Table | Generator[Table, None, None]:
        return self._list_resource(
            resource="events",
            filters=filters,
            params=params,
            all_results=all_results,
            **kwargs,
        )

    def post_event(self, payload: dict, params: dict | None = None):
        # payload = {
        #     "data": {
        #         "type": "events",
        #         "attributes": {
        #             "accept_rsvps": false,
        #             "additional_rsvps_count": 20,
        #             "allow_guests": false,
        #             "attending_count": 100,
        #             "auto_response_broadcaster_id": "1",
        #             "auto_response_content": "Content of autoresponse",
        #             "auto_response_subject": "Subject of autoresponse",
        #             "capacity_count": 200,
        #             "contact_email": "jdoe@work.com",
        #             "contact_email_private": false,
        #             "contact_name": "John Doe",
        #             "contact_phone_number": "5555555555",
        #             "contact_phone_private": false,
        #             "content": "Event description",
        #             "donation_tracking_code_id": "1",
        #             "duration": 3600,
        #             "event_form_address": "required",
        #             "event_form_phone": "required",
        #             "gather_volunteers": false,
        #             "point_person_id": "1",
        #             "private": false,
        #             "sends_auto_response": false,
        #             "show_guests": false,
        #             "start_at": "2019-10-26T10:00:00-04:00",
        #             "time_zone": "Eastern Time (US & Canada)",
        #             "user_ticket_currency": "USD",
        #             "user_ticket_price_in_cents": 400,
        #             "user_ticket_purchase_url": "https://www.example.com",
        #             "uses_shifts": false,
        #             "uses_tickets": false,
        #             "venue_name": "Event Center",
        #             "venue_address_attributes": {
        #                 "address1": "20 W 34th St.",
        #                 "address2": "Suite 100",
        #                 "address3": null,
        #                 "city": "New York",
        #                 "state": "NY",
        #                 "zip": "10001",
        #                 "county": "New York County",
        #                 "country_code": "US",
        #                 "lat": "40.7484",
        #                 "lng": "73.9857",
        #                 "fips": "04",
        #                 "submitted_address": "20 W 34th St. Suite 100, New York, NY 10001",
        #                 "distance": 0,
        #                 "import_id": "2",
        #                 "work_phone": "5555555555",
        #                 "phone_number": "5555555555",
        #                 "phone_country_code": "1",
        #                 "work_phone_number": "5555555555",
        #                 "delete": true,
        #             },
        #         },
        #         "relationships": {
        #             "page": {"data": {"type": "pages", "temp-id": "new-id", "method": "create"}}
        #         },
        #     },
        #     "included": [
        #         {
        #             "type": "pages",
        #             "temp-id": "new-id",
        #             "attributes": {
        #                 "site_id": "1",
        #                 "parent_id": "1",
        #                 "author_id": "1",
        #                 "external_id": "abc",
        #                 "slug": "your-slug",
        #                 "status": "unlisted",
        #                 "name": "Page Name",
        #                 "headline": "Page headline",
        #                 "title": "Page Title",
        #                 "excerpt": "Page excerpt...",
        #                 "page_type_name": "Basic",
        #                 "permission_level": "anyone",
        #             },
        #         }
        #     ],
        # }
        return self._post_resource(resource="events", params=params, payload=payload)

    def show_event(
        self,
        id: int | str,
        params: dict | None = None,
        sideload: list[str] | str | bool = False,
        **kwargs,
    ):
        return self._show_resource(
            resource="events", id=id, params=params, sideload=sideload, **kwargs
        )

    def delete_event(self, id: int | str, params: dict | None = None):
        return self._delete_resource(resource="events", id=id, params=params)

    def update_event(self, id: int | str, payload: dict | None = None, params: dict | None = None):
        return self._patch_resource(resource="events", id=id, params=params, payload=payload)