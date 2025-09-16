import os
import time
import concurrent.futures
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests  # Use the better library

# --- Configuration ---
PROXY_LIST_URL = "https://www.sslproxies.org/"
# We will test against a neutral site first to see if the proxy works at all.
NEUTRAL_TEST_URL = "https://api-h-c7818b61-608.sptpub.com/api/v3/prematch/brand/2420651747870650368/en/0"
# Optional: You can still test against the target site if you want, but it's less reliable for initial validation.
# TARGET_TEST_URL = "https://api-h-c7818b61-608.sptpub.com/api/v3/prematch/brand/2420651747870650368/en/0"
OUTPUT_FOLDER = "proxies"
OUTPUT_FILE = os.path.join(OUTPUT_FOLDER, "proxies.txt")
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
}
TIMEOUT = 3


def get_proxies_from_source():
    """Scrapes proxy IPs and ports from the source URL."""
    print(f"[*] Scraping proxies from {PROXY_LIST_URL}...")
    try:
        # Use a standard requests session to get the proxy list itself
        from requests import get as standard_get
        r = standard_get(PROXY_LIST_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, 'lxml')
        proxies = []
        table = soup.find("table", class_="table-striped")
        for row in table.tbody.find_all('tr'):
            ip = row.find_all('td')[0].string
            port = row.find_all('td')[1].string
            # The site lists 'yes' for https support
            if row.find_all('td')[6].string == 'yes':
                # The proxy format for curl_cffi is just http://, not https://
                proxy = f"http://{ip}:{port}"
                proxies.append(proxy)
        print(f"[+] Found {len(proxies)} potential HTTPS proxies.")
        return proxies
    except Exception as e:
        print(f"[!] Failed to scrape proxies: {e}")
        return []


def validate_proxy(proxy):
    """
    Checks if a single proxy is working by making a request with curl_cffi.
    """
    proxy_dict = {
        'http': proxy,
        'https': proxy
    }
    try:
        # Use a new session for each thread to avoid conflicts
        session = cffi_requests.Session(impersonate="chrome110")
        response = session.get(NEUTRAL_TEST_URL, proxies=proxy_dict, timeout=TIMEOUT)

        if response.status_code == 200:
            print(f"  [GOOD] {proxy} works!")
            return proxy
        else:
            # This is unlikely with httpbin but good practice
            # print(f"  [BAD] {proxy} - Status: {response.status_code}")
            return None

    except cffi_requests.errors.RequestsError as e:
        # This gives us the specific reason for failure
        error_type = type(e).__name__
        # print(f"  [BAD] {proxy} - Error: {error_type}") # Uncomment for very verbose debugging
        return None
    except Exception:
        # Catch any other unexpected errors
        return None


def main():
    """Main function to find, validate, and save proxies."""
    potential_proxies = get_proxies_from_source()
    if not potential_proxies:
        print("[!] No proxies found. Exiting.")
        return

    print(f"\n[*] Validating {len(potential_proxies)} proxies against {NEUTRAL_TEST_URL} (this may take a while)...")
    working_proxies = []

    # Using ThreadPoolExecutor to test proxies in parallel for speed
    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        future_to_proxy = {executor.submit(validate_proxy, proxy): proxy for proxy in potential_proxies}
        for future in concurrent.futures.as_completed(future_to_proxy):
            result = future.result()
            if result:
                working_proxies.append(result)

    if not working_proxies:
        print(
            "\n[!] No working proxies found after validation. The proxies from the source are likely all offline or too slow.")
        print("[*] Try running the script again, or consider paid proxies for better reliability.")
        return

    print(f"\n[+] Found {len(working_proxies)} working proxies.")

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    with open(OUTPUT_FILE, 'w') as f:
        for proxy in working_proxies:
            # The main script expects https:// for the proxy dict, so we convert it back here
            f.write(f"{proxy.replace('http://', 'https://')}\n")

    print(f"[*] Successfully saved working proxies to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()