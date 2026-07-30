---
title: "GPT and I doing a little selenium automation together, WIP"
slug: "gpt-and-i-doing-a-little-selenium-automation-together-wip"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/25"
source_id: "node-25"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
---

Selenium Bot:

A collaboration with ChatGPT and myself this addresses a rather common need to have a simple load generation for a lab or development environment. Some concepts are still needing work. So this version assumes you've got a minmal desktop environment perhaps a VM Running CentOS 7.

[embedded image skipped: data URI was too large for staged Markdown]

Additional changes to your base installation:

```
sudo yum -y install epel-release
sudo yum -y install python36 python36-pip
sudo curl https://intoli.com/install-google-chrome.sh | bash
sudo yum -y install unzip
wget https://chromedriver.storage.googleapis.com/88.0.4324.96/chromedriver_linux64.zip
unzip chromedriver_linux64.zip
sudo mv chromedriver /usr/bin/chromedriver
sudo chown root:root /usr/bin/chromedriver
sudo chmod +x /usr/bin/chromedriver
git clone https://dtlab.auzietek.com/auzieman/selenium_bot.git
cd selenium-bot
pip3 install -r requirements.txt
```

See my AuzieTek Git repo for other ideas and the current readme: [https://dtlab.auzietek.com/auzieman/selenium_bot](https://dtlab.auzietek.com/auzieman/selenium_bot)

Usage example:

```
export SELENIUM_SCRIPT_FORKS=2
python3 selenium_bot.py
======================== 1 passed, 2 warnings in 3.99s =========================
.                       [100%]

=============================== warnings summary ===============================
../../../../../usr/lib/python3.10/site-packages/_pytest/cacheprovider.py:428
  /usr/lib/python3.10/site-packages/_pytest/cacheprovider.py:428: PytestCacheWarning: could not create cache path /selenium_bot/.pytest_cache/v/cache/nodeids
    config.cache.set("cache/nodeids", sorted(self.cached_nodeids))

../../../../../usr/lib/python3.10/site-packages/_pytest/stepwise.py:49
  /usr/lib/python3.10/site-packages/_pytest/stepwise.py:49: PytestCacheWarning: could not create cache path /selenium_bot/.pytest_cache/v/cache/stepwise
    session.config.cache.set(STEPWISE_CACHE_DIR, [])

-- Docs: https://docs.pytest.org/en/stable/warnings.html
======================== 1 passed, 2 warnings in 4.19s =========================
.                              [100%]

=============================== warnings summary ===============================
../../../../../usr/lib/python3.10/site-packages/_pytest/cacheprovider.py:428
  /usr/lib/python3.10/site-packages/_pytest/cacheprovider.py:428: PytestCacheWarning: could not create cache path /selenium_bot/.pytest_cache/v/cache/nodeids
    config.cache.set("cache/nodeids", sorted(self.cached_nodeids))

../../../../../usr/lib/python3.10/site-packages/_pytest/stepwise.py:49
  /usr/lib/python3.10/site-packages/_pytest/stepwise.py:49: PytestCacheWarning: could not create cache path /selenium_bot/.pytest_cache/v/cache/stepwise
    session.config.cache.set(STEPWISE_CACHE_DIR, [])

-- Docs: https://docs.pytest.org/en/stable/warnings.html
======================== 1 passed, 2 warnings in 4.57s =========================
[
    {
        "script": "test_redminequick.py",
        "fork": 1,
        "start_time": 1678743147.718879,
        "end_time": 1678743157.756006,
        "exit_status": "ERROR",
        "pid": 528780,
        "current_time": null
    },
    {
        "script": "test_quick.py",
        "fork": 1,
        "start_time": 1678743147.71961,
        "end_time": 1678743157.775154,
        "exit_status": "ERROR",
        "pid": 528781,
        "current_time": null
    },
    {
        "script": "test_lightweight.py",
        "fork": 1,
        "start_time": 1678743147.720332,
        "end_time": 1678743157.791993,
        "exit_status": "ERROR",
        "pid": 528782,
        "current_time": null
    }
]
```

selenium_bot.py code:

```
import os
import subprocess
import time
from datetime import datetime
import json

def main():
    script_dir = os.environ.get('SELENIUM_SCRIPT_DIR', '/selenium_bot/')
    interval = int(os.environ.get('SELENIUM_SCRIPT_INTERVAL', '10'))
    forks = int(os.environ.get('SELENIUM_SCRIPT_FORKS', '1'))
    chromedriver_path = os.environ.get('CHROMEDRIVER_PATH', '/usr/bin/chromedriver')

    while True:
        scripts = [f for f in os.listdir(script_dir) if f.endswith('.py')]
        runs = []
        for script in scripts:
            for i in range(1, forks + 1):
                proc_env = os.environ.copy()
                proc_env['SELENIUM_FORK_NUM'] = str(i)
                proc_env['PATH'] = f"{proc_env['PATH']}:{chromedriver_path}"
                proc = subprocess.Popen(
                    ["pytest", f"{script_dir}/{script}"],
                    env=proc_env
                )
                runs.append({
                    "script": script,
                    "fork": i,
                    "start_time": datetime.now().timestamp(),
                    "end_time": None,
                    "exit_status": None,
                    "pid": proc.pid
                })
        time.sleep(interval)
        for run in runs:
            if run["end_time"] is not None:
                continue
            proc = subprocess.Popen(
                ["ps", "-o", "etime=", "-o", "stat=", "-p", str(run["pid"])],
                stdout=subprocess.PIPE
            )
            out, _ = proc.communicate()
            if proc.returncode == 0:
                run["current_time"] = out.decode().strip().split(" ")[0]
                run["exit_status"] = out.decode().strip().split(" ")[1]
                run["end_time"] = datetime.now().timestamp()
            else:
                run["current_time"] = None
                run["exit_status"] = "ERROR"
                run["end_time"] = datetime.now().timestamp()

        # Print the results as JSON
        print(json.dumps(runs, indent=4, default=str))

if __name__ == '__main__':
    main()
```

Somethings went smooth others didn't with a bit more work this can be done inside a container, however its easier to do it on an x11 minimal workstation.  Further changes, rather than using the .side format export tests to python. This will spawn chrome windows running tests, if the tests have no issues a health status and some timing is returned.

Later versions will probably run headless but additional research is required.

Blog tags

[selenium](/taxonomy/term/40)

[docker](/taxonomy/term/11)

[ChatGPT](/taxonomy/term/23)

[Python](/taxonomy/term/21)

Submitted by auzieman
 on Sat, 03/11/2023 - 15:59

