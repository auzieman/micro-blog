---
title: "A second lighter weight traffic generator written by GPT and myself."
slug: "a-second-lighter-weight-traffic-generator-written-by-gpt-and-myself"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/26"
source_id: "node-26"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
---

# Requests Spider

Requests Spider is a simple Python script that crawls the links found in a list of URLs and returns the response times for each link. It uses the Requests library to fetch the HTML for each URL and BeautifulSoup to parse the HTML and extract the links.

[embedded image skipped: data URI was too large for staged Markdown]

Repository has more recent information [DTLab AuzieTek GIT](https://dtlab.auzietek.com/auzieman/requests_spider)

A recent version of the code.

```
import os
from bs4 import BeautifulSoup
import json
import sys
import time
import requests
from datetime import datetime

def crawl_url(url):
    """
    Crawls the given URL and returns the response time and any internal links found.
    """
    start_time = datetime.now()
    response = requests.get(url)

    soup = None
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

    internal_links = []
    if soup:
        for link in soup.find_all('a'):
            href = link.get('href')
            if href and not href.startswith('http'):
                internal_links.append(href)

    response_time = (datetime.now() - start_time).total_seconds() * 1000
    return response_time, internal_links

def main():
    script_dir = os.environ.get('REQUESTS_SPIDER_DIR', '/requests_spider/')
    interval = int(os.environ.get('REQUEST_SPIDER_INTERVAL', '10'))
    forks = int(os.environ.get('REQUEST_SPIDER_FORKS', '1'))

    while True:
        urls_file = os.path.join(script_dir, 'urls.json')
        if not os.path.isfile(urls_file):
            print(f"Error: {urls_file} not found.")
            sys.exit(1)

        with open(urls_file) as f:
            urls = json.load(f)

        runs = []
        for url in urls:
            for i in range(1, forks + 1):
                time.sleep(0.5) # add a delay to be polite
                response_time, internal_links = crawl_url(url)
                runs.append({
                    "url": url,
                    "fork": i,
                    "start_time": datetime.now().timestamp(),
                    "end_time": None,
                    "exit_status": None,
                    "response_time": response_time,
                    "internal_links": internal_links,
                })

        time.sleep(interval)

        for run in runs:
            if run["end_time"] is not None:
                continue
            run["end_time"] = datetime.now().timestamp()

        # Print the results
        for run in runs:
            print(f"URL: {run['url']}\tFork: {run['fork']}\tStart Time: {datetime.fromtimestamp(run['start_time'])}\tEnd Time: {datetime.fromtimestamp(run['end_time'])}\tResponse Time: {run['response_time']}ms\tInternal Links: {run['internal_links']}")

if __name__ == '__main__':
    main()
```

## Getting Started

### Prerequisites

To run the Requests Spider script, you will need:

- Python 3.x
- Requests library
- BeautifulSoup library

### Installation

1. Clone the repository: `git clone https://github.com/your-username/requests_spider.git`
2. Install the required libraries: `pip install -r requirements.txt`

### Usage

The Requests Spider script can be run from the command line using the following syntax:

python requests_spider.py [-h] [-f FORKS] [-d DELAY] [-v] url_file

where:

- `url_file` is the path to the JSON file containing the URLs to crawl
- `-h` shows the help message and exits
- `-f FORKS` sets the number of forks to use (default is 1)
- `-d DELAY` sets the delay between each loop in seconds (default is 10)
- `-v` enables verbose mode, which displays additional output

### Example

To run the Requests Spider script with 2 forks and a delay of 5 seconds between loops, use the following command:

python requests_spider.py -f 2 -d 5 urls.json

## License

This project is licensed under the MIT License - see the [LICENSE.md](https://dtlab.auzietek.com/auzieman/requests_spider/src/master/LICENSE.md) file for details.

## Acknowledgments

- Requests library: [https://requests.readthedocs.io/](https://requests.readthedocs.io/)
- BeautifulSoup library: [https://www.crummy.com/software/BeautifulSoup/bs4/doc/](https://www.crummy.com/software/BeautifulSoup/bs4/doc/)

The included Docker file on this repository could be configured to run this job in the background.

Blog tags

[Python](/taxonomy/term/21)

[ChatGPT](/taxonomy/term/23)

[linux](/taxonomy/term/7)

[docker](/taxonomy/term/11)

Submitted by auzieman
 on Mon, 03/13/2023 - 14:50

