from parsons.utilities import check_env
from parsons.utilities.api_connector import APIConnector

class NBConnector:

    def __init__(self, uri, headers, pagination_key):
        self.uri = uri
        self.pagination_key = pagination_key
        self.api = APIConnector(
            self.uri,
            headers=headers,
            pagination_key=self.pagination_key,
        )

    def get_request(self, endpoint, params=None, **kwargs):
        params = NBConnector.normalize_params(params)
        resp = self.api.get_request(url=endpoint, params=params,**kwargs)
        data = resp.json()["data"]

        # Paginate
        while isinstance(r, dict) and self.api.next_page_check_url(r):
            if endpoint == "savedLists" and not r["items"]:
                break
            if endpoint == "printedLists" and not r["items"]:
                break
            r = self.api.get_request(r[self.pagination_key], **kwargs)
            data.extend(self.api.data_parse(r))
        return data

    def post_request(self, endpoint, **kwargs):
        return self.api.post_request(endpoint, **kwargs)

    def delete_request(self, endpoint, **kwargs):
        return self.api.delete_request(endpoint, **kwargs)

    def patch_request(self, endpoint, **kwargs):
        return self.api.patch_request(endpoint, **kwargs)

    def put_request(self, endpoint, **kwargs):
        return self.api.put_request(endpoint, **kwargs)

    @classmethod
    def normalize_params(cls, data: dict):
        normal_list = lambda x: ",".join(x) if len(x) > 0 else None

        temp = {}
        for i in data:
            if type(data[i]) is dict:

                for key, val in data[i].items():
                    if type(val) is list or type(val) is tuple or type(val) is set:
                        temp[f"{i}[{key}]"] = normal_list(val)
                    else:
                        temp[f"{i}[{key}]"] = val

            elif type(data[i]) is list:
                temp[i] = normal_list(data[i])

            else:
                temp[i] = data[i]

        # return temp
        return {key: val for key, val in temp.items() if val is not None}

    @classmethod
    def paginate(resp):
        try:
            return resp.json()["links"]["next"]
        except:
            return False