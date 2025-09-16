import re
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_ddos_protection_cookie(session):
    headers_get = {
        "Host": "tounesbet.com",
        "Cookie": "_culture=en-us; TimeZone=-60;",
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    resp = session.get("https://tounesbet.com/", headers=headers_get, verify=False, timeout=30)
    match = re.search(r'DDoS_Protection=([0-9a-f]+)', resp.text)
    if match:
        return match.group(1)
    else:
        raise RuntimeError("DDoS_Protection cookie not found")

def fetch_live_html():
    session = requests.Session()

    # Step 1: GET to retrieve and set the DDoS_Protection cookie
    ddos_val = get_ddos_protection_cookie(session)
    session.cookies.set("DDoS_Protection", ddos_val, domain="tounesbet.com", path="/")

    # Step 2: POST to /Live with the cookie
    headers_post = {
        "Host": "tounesbet.com",
        "Cookie": f"_culture=en-us; TimeZone=-60; DDoS_Protection={ddos_val}",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": "Mozilla/5.0",
        "Accept": "*/*",
        "Origin": "https://tounesbet.com",
        "Referer": "https://tounesbet.com/?d=1",
    }
    body = "SportId=1181&Page=1&PageSize=40&Patern="

    resp = session.post(
        "https://tounesbet.com/Live",
        headers=headers_post,
        data=body,
        verify=False,
        timeout=30
    )

    # Step 3: Write raw HTML response to response.html
    with open("response.html", "w", encoding="utf-8") as f:
        f.write(resp.text)

if __name__ == "__main__":
    fetch_live_html()
