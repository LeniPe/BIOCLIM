import os
import time
import requests
import tempfile
from destinelab import AuthHandler  # type: ignore


def get_days_in_month(year: int, month: int):
    """Returns the number of days in a given month, accounting for leap years."""
    if month in [1, 3, 5, 7, 8, 10, 12]:
        return 31
    elif month in [4, 6, 9, 11]:
        return 30
    elif month == 2:
        # Check for leap year
        if (year % 4 == 0 and year % 100 != 0) or (year % 400 == 0):
            return 29
        else:
            return 28
    else:
        raise ValueError("Invalid month")


class DEDLDownloader:
    def __init__(self, user=None, pwd=None):
        self.user = user or os.environ.get("DEDL_USER")
        self.pwd = pwd or os.environ.get("DEDL_PWD")

        if not self.user or not self.pwd:
            raise ValueError("DEDLDownloader requires user and pwd credentials")

        self.auth = AuthHandler(self.user, self.pwd)
        self.base_url = "https://hda.data.destination-earth.eu/stac/v2/"
        self.session = requests.Session()
        self.update_token()

    def update_token(self):
        """Refreshes the Bearer token."""
        token = self.auth.get_token()
        self.session.headers.update({"Authorization": f"Bearer {token}"})

    def request_with_retry(
        self,
        method: str,
        url: str,
        *,
        error_message: str,
        expected_status: int | tuple[int, ...] = 200,
        max_retries: int = 3,
        retry_delay: int = 5,
        **kwargs,
    ) -> requests.Response:
        """Sends an HTTP request with retry logic and customizable error messages."""
        expected_statuses = (
            (expected_status,) if isinstance(expected_status, int) else expected_status
        )

        last_error: Exception | None = None
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.request(
                    method=method.upper(), url=url, **kwargs
                )

                if response.status_code in expected_statuses:
                    return response

                last_error = Exception(
                    f"{error_message}: {response.status_code} - {response.text}"
                )
            except requests.RequestException as e:
                last_error = Exception(f"{error_message}: {e}")

            if attempt < max_retries:
                time.sleep(retry_delay)

        raise Exception(
            f"{error_message} after {max_retries} attempts"
            + (f". Last error: {last_error}" if last_error else "")
        )

    def build_order_payload(
        self, year: int, month: int, variable: str, collection: str
    ):
        """Constructs the payload for ordering data."""
        if collection == "EO.ECMWF.DAT.ERA5_LAND_MONTHLY":
            payload = {
                "variable": [variable],
                "month": [f"{m:02d}" for m in range(1, 13)],
                "year": [str(year)],
                "time": ["00:00"],
                "download_format": "unarchived",
                "data_format": "netcdf",
                "product_type": ["monthly_averaged_reanalysis"],
            }
        elif collection == "EO.ECMWF.DAT.ERA5_LAND_HOURLY":
            days = get_days_in_month(year, month)
            payload = {
                "variable": [variable],
                "month": f"{month:02d}",
                "year": str(year),
                "day": [f"{d:02d}" for d in range(1, days + 1)],
                "time": [f"{h:02d}:00" for h in range(24)],
                "download_format": "unarchived",
                "data_format": "netcdf",
            }
        else:
            raise ValueError(f"Unsupported collection: {collection}")
        return payload

    def submit_order_request(
        self, year: int, month: int, variable: str, collection: str
    ):
        """Orders data and polls until the download link is ready."""
        order_url = f"{self.base_url}collections/{collection}/order"

        # Initial POST to order the data
        payload = self.build_order_payload(year, month, variable, collection)
        resp = self.request_with_retry(
            "POST",
            order_url,
            json=payload,
            error_message="Order failed",
            expected_status=200,
            max_retries=1,
            retry_delay=20,
        )

        order_info = resp.json()
        # Find the 'self' link to track status
        status_url: str = next(
            link["href"] for link in order_info["links"] if link["rel"] == "self"
        )

        return status_url

    def poll_order_and_download(self, status_url: str, save_path: str, month: str):
        """Polls a single order until finished, then streams the download."""
        print(f"⏳ Polling order {month}... ", end="\r", flush=True)
        start = time.time()
        while True:
            status_resp = self.request_with_retry(
                "GET",
                status_url,
                error_message="Failed to get order status",
                expected_status=200,
                max_retries=3,
                retry_delay=20,
            )
            data = status_resp.json()

            status = data["properties"].get("order:status")

            if status == "succeeded":
                print("\n✅ Order succeeded, downloading...", flush=True)
                download_url = data["assets"]["downloadLink"]["href"]

                for attempt in range(1, 4):
                    try:
                        r = self.session.get(download_url, stream=True)
                        with r:
                            r.raise_for_status()
                            target_dir = os.path.dirname(save_path) or "."

                            tmp_path = None
                            try:
                                with tempfile.NamedTemporaryFile(
                                    dir=target_dir, delete=False
                                ) as tmp:
                                    tmp_path = tmp.name
                                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                                        if chunk:
                                            tmp.write(chunk)

                                # Atomically move the temp file into place
                                os.replace(tmp_path, save_path)
                                print(f"💾 Downloaded to {save_path}", flush=True)
                                return True
                            except Exception:
                                # Cleanup temporary file if it still exists
                                if tmp_path and os.path.exists(tmp_path):
                                    os.remove(tmp_path)

                    except Exception as e:
                        print(
                            f"    ❌ Failed to download month {month}, attempt {attempt}: {e}"
                        )
                        if attempt == 3:
                            return False
                        time.sleep(20)
            elif status == "failed":
                print("\n❌ Order failed", flush=True)
                return False
            stop = time.time()
            print(
                f"⏳ Polling order {month}... Status: {status}, Elapsed: {(stop - start) / 60:.2f} minutes",
                end="\r",
                flush=True,
            )
            if (stop - start) > 30 * 60:
                print("\n⏰ Order polling timed out", flush=True)
                return False
            time.sleep(30)

    def download_year(
        self, year: int, variable: str, collection: str, output_dir: str = "./data/raw"
    ) -> list[bool]:
        pending_orders: list[dict[str, str]] = []

        if collection == "EO.ECMWF.DAT.ERA5_LAND_HOURLY":
            monthly_splits = True
        elif collection == "EO.ECMWF.DAT.ERA5_LAND_MONTHLY":
            monthly_splits = False
        else:
            raise ValueError(f"Unsupported collection: {collection}")

        print(f"🚀 Ordering {variable} data for {year}...")
        if monthly_splits:
            for month in range(1, 13):
                m_str = f"{month:02d}"
                save_path = os.path.join(output_dir, f"{variable}_{year}_{m_str}.nc")
                if os.path.exists(save_path):
                    print(f"    ✅ Month {m_str} already exists, skipping order.")
                    continue
                try:
                    status_url = self.submit_order_request(
                        year, month, variable, collection
                    )
                    pending_orders.append(
                        {"url": status_url, "path": save_path, "month": m_str}
                    )
                except Exception as e:
                    print(f"    ❌ Failed to order month {month}: {e}")
                else:
                    print(f"    📦 Ordered month {m_str}")
        else:
            save_path = os.path.join(output_dir, f"{variable}_{year}.nc")
            if os.path.exists(save_path):
                print(f"    ✅ Year {year} already exists, skipping order.")
                return []
            try:
                status_url = self.submit_order_request(year, 0, variable, collection)
                pending_orders.append(
                    {"url": status_url, "path": save_path, "month": "full_year"}
                )
            except Exception as e:
                print(f"    ❌ Failed to order year {year}: {e}")
            else:
                print(f"    📦 Ordered year {year}")

        if len(pending_orders) == 0:
            print("🎉 All data already downloaded!")
            return []
        print("⏳ Monitoring queue and downloading...")
        results = []

        for order in pending_orders:
            success = self.poll_order_and_download(
                order["url"], order["path"], order["month"]
            )
            results.append(success)

        return results
