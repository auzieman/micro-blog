---
title: Dynatrace Managed Notes: SELinux, Ports, and Practical Requirements
slug: dynatrace-managed-selinux-ports-practical-requirements
summary: A preserved Linux operations note about Dynatrace Managed prerequisites, SELinux evidence, firewall ports, and why monitoring platforms need careful host preparation.
tags: [linux, dynatrace, selinux, firewall, monitoring, operations]
theme_variant: linux-pro
status: published
seo_title: Dynatrace Managed SELinux and firewall requirements
seo_description: Practical Linux notes for Dynatrace Managed around SELinux policies, required ports, firewall rules, and host preparation.
canonical_url: https://auzietek.com/node/28
---

Monitoring platforms ask a lot from the hosts they run on.

Dynatrace Managed is no exception. The installer applies a large number of
SELinux policies because Dynatrace needs extensive system-level access for
monitoring tasks. Most of the policy footprint is related to OneAgent, but
Managed components and gateways also need carefully prepared host access.

The old Auzietek note was partly a warning: do not treat monitoring software as
“just another package.” Read what it changes. Verify the host. Keep the evidence.

## SELinux discovery commands

If SELinux is enabled, useful first-pass commands include:

```bash
semanage port -l
semanage permissive -l
semanage module -l
semanage fcontext -l
seinfo
```

`sesearch` is also useful when you need to inspect policy rules for specific
types of access, such as network or file access.

Even when you are not deep in SELinux tooling, a simple recursive search can
tell you a lot:

```text
# fgrep -i "dynatrace" -R /etc/selinux/
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/log(/.*)?	unconfined_u:object_r:oneagent_log_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/var/lib/dynatrace/oneagent(/.*)?	unconfined_u:object_r:oneagent_var_lib_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/var/log/dynatrace/oneagent(/.*)?	unconfined_u:object_r:oneagent_log_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/log/(crashreports|memorydump|supportalerts)(/.*)?	unconfined_u:object_r:oneagent_datastorage_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/agent(/.*)?	unconfined_u:object_r:oneagent_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/log/process(/.*)?	unconfined_u:object_r:oneagent_process_log_t:s0
```

That output gives an operator a starting point: paths, labels, and the rough
shape of what the installer placed on the system.

## Firewall evidence

The original note also preserved iptables evidence because Dynatrace Managed
clusters need several internal and external ports to behave correctly:

```text
# iptables -L --numeric
Chain INPUT (policy ACCEPT)
target     prot opt source               destination
ACCEPT     all  --  0.0.0.0/0            0.0.0.0/0            state RELATED,ESTABLISHED
ACCEPT     tcp  --  0.0.0.0/0            0.0.0.0/0            tcp multiport ports 443,8021:8022,8443
ACCEPT     tcp  --  192.168.1.12         0.0.0.0/0            tcp multiport ports 5701:5711,7000:7001,8019:8020,9042,9200,9300,9998
ACCEPT     tcp  --  0.0.0.0/0            0.0.0.0/0            ADDRTYPE match src-type LOCAL tcp multiport ports 443,5701:5711,7000:7001,7199,8018:8022,8443,9042,9200,9300,9998
DROP       tcp  --  0.0.0.0/0            0.0.0.0/0            tcp multiport ports 443,5701:5711,7000:7001,7199,8018:8022,8443,9042,9200,9300,9998
```

The exact ports can change by product version and topology, so do not copy this
blindly into a modern deployment. Use it as an example of the evidence to
capture:

- public UI/API ports;
- internal cluster ports;
- local-only allowances;
- gateway communication;
- storage/search/database ports;
- explicit drops after expected allows.

## The practical lesson

SELinux and firewall rules are often where “the installer ran” diverges from
“the platform is healthy.”

For monitoring tools, the host is part of the product. Before blaming the
application, inspect the operating-system contract:

```bash
getenforce
semanage module -l | grep -i dynatrace
ss -lntup
iptables -L --numeric
journalctl -u dynatrace* --no-pager
```

That is the Linux Users teaching angle: preserve the actual commands and
evidence, then explain why they matter. Monitoring should make systems easier to
understand, not add one more invisible layer of mystery.

Legacy source: [Auzietek Dynatrace Managed notes](https://auzietek.com/node/28).
