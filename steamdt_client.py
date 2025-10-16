import os
import requests


class SteamDTClient:
    BASE_URL = "https://open.steamdt.com"

    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.getenv("STEAMDT_API_KEY")
        self.session = requests.Session()
        if self.api_key:
            self.session.headers.update({
                "Authorization": f"Bearer {self.api_key}",
            })

    def _ensure_key(self):
        if not self.api_key:
            raise RuntimeError("未配置 STEAMDT_API_KEY，请在环境变量中设置后重试。")

    def get_base_info(self):
        """GET /open/cs2/v1/base (每日 1 次)
        返回包含 name, marketHashName, platformList[{ name, itemId }]
        文档: https://doc.steamdt.com/278832832e0
        """
        self._ensure_key()
        url = f"{self.BASE_URL}/open/cs2/v1/base"
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_price_single(self, market_hash_name: str):
        """GET /open/cs2/v1/price/single?marketHashName=xxx (每分钟 60 次)
        返回各平台最新价格信息
        文档汇总: https://doc.steamdt.com/6369437m0
        """
        self._ensure_key()
        url = f"{self.BASE_URL}/open/cs2/v1/price/single"
        params = {"marketHashName": market_hash_name}
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_price_batch(self, market_hash_names: list[str]):
        """POST /open/cs2/v1/price/batch
        Body: {"marketHashNames": ["..."]} (1-100)
        文档: https://doc.steamdt.com/278832831e0
        """
        self._ensure_key()
        url = f"{self.BASE_URL}/open/cs2/v1/price/batch"
        json_body = {"marketHashNames": market_hash_names}
        resp = self.session.post(url, json=json_body, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def get_price_avg(self, market_hash_name: str):
        """GET /open/cs2/v1/price/avg?marketHashName=xxx
        返回近7天所有平台均价以及分平台均价
        文档: https://doc.steamdt.com/319748133e0
        """
        self._ensure_key()
        url = f"{self.BASE_URL}/open/cs2/v1/price/avg"
        params = {"marketHashName": market_hash_name}
        resp = self.session.get(url, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()