from bs4 import BeautifulSoup
import requests
import re
from typing import Dict, Set

def paragraph_mentions(text: str, keyword: str) -> bool:
    soup = BeautifulSoup(text, 'html5lib')
    paragraphs = [p.get_text() for p in soup('p')]
    
    return any(keyword.lower() in paragraphs.lower()
            for p in paragraphs)

url = "https://www.house.gov/representatives"
html = requests.get(url).text
soup = BeautifulSoup(html, 'html5lib')

all_urls = [a['href']
    for a in soup('a')
    if a.has_attr('href')]

regex = r'^https?://.*\.house\.gov/?$'

assert re.match(regex, "http://joel.house.gov")
assert not re.match(regex, "http://joel.house.com")

good_urls = [url for url in all_urls if re.match(regex, url)]
good_urls = list(set(good_urls))

press_releases: Dict[str, Set[str]] = {}

for house_url in good_urls:
    html = requests.get(house_url).text
    soup = BeautifulSoup(html, 'html5lib')
    pr_links = [a['href'] for a in soup('a')
                if 'press releases' in a.text.lower()]
    #print(f"{house_url} : {pr_links}")
    press_releases[house_url] = pr_links

for house_url, pr_links in press_releases.items():
    for pr_link in pr_links:
        try:
            html = requests.get(pr_link, timeout=5).text
            
            if paragraph_mentions(html, 'data'):
                print(f"Encontrado em: {house_url}")
                break
        except requests.RequestException:
            continue

"""print(len(good_urls))

print(len(all_urls))"""