---
title: "Configuring Log Retention and Customizing Settings for Dynatrace OneAgent on AIX"
slug: "configuring-log-retention-and-customizing-settings-for-dynatrace-oneagent-on-aix"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/29"
source_id: "node-29"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Introduction

Dynatrace OneAgent is an application performance monitoring (APM) solution that collects various types of data, including log files, from your system. In less common installation cases, such as AIX systems, you might want to configure log retention settings, customize installation paths, and adjust monitoring options like full-stack or infrastructure-only monitoring. This article will cover these customization options for Dynatrace OneAgent on AIX systems.

Configuring Log Retention

To limit log retention on AIX systems, you can set up log rotation using the `logrotate` utility. Here's the basic configuration for log rotation:

- Install logrotate

```
Make sure its installed,

rpm -i logrotate-3.x.x-x.aix6.1.ppc.rpm
```

- Identify the log files location

```
ls -laR /var/log/dynatrace/oneagent/
```

- Create a logrotate configuration file

```
vi /etc/logrotate.conf

include /etc/logrotate.d
```

- Schedule logrotate

```
Put the service into crontab,

crontab -e
0 0 * * * /usr/sbin/logrotate /etc/logrotate.conf
```

- Monitor and adjust

```
Setup a rule to handle the log path,
vi /etc/logrotate.d/dynatrace-oneagent

/var/log/dynatrace/oneagent/*.log {
    daily
    rotate 1
    compress
    missingok
    notifempty
    size 10M
    postrotate
        /opt/dynatrace/oneagent/agent/initscripts/oneagent stop >/dev/null 2>&1 || true
        /opt/dynatrace/oneagent/agent/initscripts/oneagent start >/dev/null 2>&1 || true
    endscript
}
```

- Full-Stack vs. Infrastructure-Only Monitoring

By default, Dynatrace OneAgent performs full-stack monitoring, which covers all aspects of your environment, including applications, services, processes, and infrastructure components. In some cases, you might prefer infrastructure-only monitoring, which focuses only on the infrastructure components and omits application-level details.

To switch between full-stack and infrastructure-only monitoring, you can use the `--set-infra-only` or `--set-full-stack` options when installing or updating OneAgent:

```
./Dynatrace-OneAgent-AIX-1.0.sh --set-infra-only
```

or

```
 
```

```
./Dynatrace-OneAgent-AIX-1.0.sh --set-full-stack
```

- Limiting Auto-Injection

Dynatrace OneAgent automatically injects monitoring capabilities into supported applications and services. In some cases, you may want to limit the auto-injection process to prevent monitoring certain applications. To achieve this, you can use the `--skip` option during installation:

```
./Dynatrace-OneAgent-AIX-1.0.sh --skip=java,nodejs,php
```

This command would prevent auto-injection for Java, Node.js, and PHP applications.

- Customizing Installation, Log, and Var Paths

Dynatrace OneAgent allows you to customize the installation, log, and var paths during installation. To do this, use the following options:

- `--install-dir`: Specifies the custom installation directory.
- `--log-dir`: Specifies the custom log directory.
- `--var-dir`: Specifies the custom var directory.

For example, to install Dynatrace OneAgent in `/custom/path/dynatrace/oneagent`, with custom log and var directories, you can run:

```
./Dynatrace-OneAgent-AIX-1.0.sh --install-dir=/custom/path/dynatrace/oneagent --log-dir=/custom/path/dynatrace/logs --var-dir=/custom/path/dynatrace/var
```

Make sure to update your logrotate configuration and other settings to reflect the custom paths.

Conclusion

This article covered the customization options for Dynatrace OneAgent on AIX systems, including configuring log retention, adjusting monitoring options, limiting auto-injection, and customizing installation paths. It's essential to consult Dynatrace's official documentation or contact their support for the best practices specific to AIX systems and your unique installation.

Blog tags

[AIX](/taxonomy/term/43)

[OneAgent](/taxonomy/term/44)

[logrotate](/taxonomy/term/45)

[Dynatrace](/taxonomy/term/17)

Submitted by auzieman
 on Fri, 03/24/2023 - 15:34

