---
title: "Introducing Container Tamer: Streamlined Container Configuration Management"
slug: "introducing-container-tamer-streamlined-container-configuration-management"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/30"
source_id: "node-30"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Introduction:

Pet Project, still in early stages.

This rather clever yet simple bit of python can be added to almost any container

Main code,

```
#!/usr/bin/env python

import json
import os
from jinja2 import Environment, FileSystemLoader

# Load configuration from config.json
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

dictfile = {}

# Read variables from the config dictionary
for key, value in config.items():
    if key.startswith("TPL_"):
        dictfile[key] = value

def do_template(dict, out, templ, run_script=False):
    env = Environment(loader=FileSystemLoader('./'))
    template = env.get_template(templ)
    rendered_content = template.render(dict)
    print("Dictionary applied", json.dumps(dict, indent=4), "To, ", out)
    with open(out, 'w') as outfile:
        outfile.write(rendered_content)
    if run_script:
        os.chmod(out, 0o755)
        os.system(out)

# Run jinja2 templates
templates = config["JINJA_TEMP"]
for item in templates:
    out_tpl = item[0]
    in_tpl = item[1]
    run_script = item[2] if len(item) > 2 else False
    do_template(dictfile, out_tpl, in_tpl, run_script=run_script)
```

You can see the example json and templates in the repository.

Managing container configurations can be a challenging task. Maintaining consistent configurations across various environments while keeping up with the fast-paced nature of modern development practices can be difficult. That's where Container Tamer comes in.

Container Tamer is a flexible and user-friendly solution designed to simplify container configuration management. By leveraging the power of Jinja2 templates and JSON configuration files, Container Tamer streamlines the process of configuring and updating containerized applications like Drupal.

Repository: [https://dtlab.auzietek.com/auzieman/container_tamer/](https://dtlab.auzietek.com/auzieman/container_tamer/) (main active area is examples/drupal)

Main Features:

1. **Jinja2 Templating**: Container Tamer utilizes Jinja2 templates to manage configurations, making it easy to customize your container settings. You can define various configuration parameters as placeholders within your templates and dynamically render configurations based on your JSON config files.
2. **JSON Configuration Files**: Container Tamer leverages JSON files to store all relevant configuration settings. This approach offers a simple, human-readable way to manage settings and allows you to easily share and maintain configurations across different environments.
3. **Automatic Script Execution**: The latest version of Container Tamer supports automatic script execution after applying the Jinja2 template. This feature simplifies the process of initializing and updating containerized applications like Drupal, automating tasks such as installing and updating modules.

Example Use Case: Drupal Container Initialization

Container Tamer can be used to streamline the configuration and initialization process for Drupal containers. The following example demonstrates how to provision or update Drupal's default container on startup:

1. Use Jinja2 templates to define configuration files like `settings.php`, `pre_flight-test.php`, and `drush_init.sh`.
2. Define a JSON configuration file (`config.json`) that contains all relevant settings, such as database connection details, site information, and desired modules.
3. Run the Container Tamer script to apply the Jinja2 templates based on the JSON configuration, creating the necessary configuration files and executing any required scripts.

Conclusion:

Container Tamer is a powerful tool for simplifying container configuration management. It allows developers to easily maintain consistent configurations across various environments and streamlines the process of deploying and updating containerized applications. Give Container Tamer a try and experience a more efficient way to manage your container configurations.

Blog tags

[Jinja](/taxonomy/term/28)

[docker](/taxonomy/term/11)

[linux](/taxonomy/term/7)

Submitted by auzieman
 on Wed, 03/29/2023 - 11:34

