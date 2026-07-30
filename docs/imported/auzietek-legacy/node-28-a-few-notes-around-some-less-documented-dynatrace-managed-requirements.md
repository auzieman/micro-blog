---
title: "A few notes around some less documented Dynatrace Managed requirements."
slug: "a-few-notes-around-some-less-documented-dynatrace-managed-requirements"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/28"
source_id: "node-28"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Dynatrace Managed installer applies a large number of SELinux policies by default. This is because Dynatrace requires extensive system-level access to perform its monitoring tasks. As you noted, the policies are primarily related to the OneAgent, but there are also policies for other components of the system, such as the Dynatrace Managed gateway.

It's important to note that disabling SELinux can reduce the security of the system, as it provides an additional layer of protection against malicious attacks. However, in some cases, disabling SELinux may be necessary to resolve issues.

If you want to check the SELinux policies that are currently applied on your system, you can use the following commands:

- `semanage port -l`: Lists all ports that have been configured to work with SELinux.
- `semanage permissive -l`: Lists all processes and domains that are currently running in permissive mode (i.e., SELinux is logging policy violations but not enforcing them).
- `semanage module -l`: Lists all installed SELinux policy modules.
- `semanage fcontext -l`: Lists all file context mappings currently defined in the SELinux policy.

You can also use the `seinfo` command to display information about the current SELinux policy, such as the policy version, mode, and status. And `sesearch` is a tool that allows you to search the SELinux policy for specific types of access, such as network access or file access.

The above commands will work if you have selinux enabled, however this little trick also works.

```
# fgrep -i "dynatrace" -R /etc/selinux/
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/log(/.*)?	unconfined_u:object_r:oneagent_log_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/var/lib/dynatrace/oneagent(/.*)?	unconfined_u:object_r:oneagent_var_lib_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/var/log/dynatrace/oneagent(/.*)?	unconfined_u:object_r:oneagent_log_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/log/(crashreports|memorydump|supportalerts)(/.*)?	unconfined_u:object_r:oneagent_datastorage_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/agent(/.*)?	unconfined_u:object_r:oneagent_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/log/process(/.*)?	unconfined_u:object_r:oneagent_process_log_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/var/log/dynatrace/oneagent/process(/.*)?	unconfined_u:object_r:oneagent_process_log_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/var/lib/dynatrace/oneagent/datastorage(/.*)?	unconfined_u:object_r:oneagent_datastorage_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/var/lib/dynatrace/oneagent/agent/runtime(/.*)?	unconfined_u:object_r:oneagent_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/agent/processagent(/.*)?	unconfined_u:object_r:lib_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/agent/lib64/oneagentdynamizer	unconfined_u:object_r:oneagent_exec_t:s0
/etc/selinux/targeted/contexts/files/file_contexts:/opt/dynatrace/oneagent/agent/libmusl64/oneagentdynamizer	unconfined_u:object_r:oneagent_exec_t:s0
Binary file /etc/selinux/targeted/contexts/files/file_contexts.bin matches
/etc/selinux/targeted/active/file_contexts:/opt/dynatrace/oneagent/log(/.*)?	unconfined_u:object_r:oneagent_log_t:s0
/etc/selinux/targeted/active/file_contexts:/var/lib/dynatrace/oneagent(/.*)?	unconfined_u:object_r:oneagent_var_lib_t:s0
/etc/selinux/targeted/active/file_contexts:/var/log/dynatrace/oneagent(/.*)?	unconfined_u:object_r:oneagent_log_t:s0
/etc/selinux/targeted/active/file_contexts:/opt/dynatrace/oneagent/log/(crashreports|memorydump|supportalerts)(/.*)?	unconfined_u:object_r:oneagent_datastorage_t:s0
/etc/selinux/targeted/active/file_contexts:/opt/dynatrace/oneagent/agent(/.*)?	unconfined_u:object_r:oneagent_t:s0
/etc/selinux/targeted/active/file_contexts:/opt/dynatrace/oneagent/log/process(/.*)?	unconfined_u:object_r:oneagent_process_log_t:s0
/etc/selinux/targeted/active/file_contexts:/var/log/dynatrace/oneagent/process(/.*)?	unconfined_u:object_r:oneagent_process_log_t:s0
/etc/selinux/targeted/active/file_contexts:/var/lib/dynatrace/oneagent/datastorage(/.*)?	unconfined_u:object_r:oneagent_datastorage_t:s0
/etc/selinux/targeted/active/file_contexts:/var/lib/dynatrace/oneagent/agent/runtime(/.*)?	unconfined_u:object_r:oneagent_t:s0
/etc/selinux/targeted/active/file_contexts:/opt/dynatrace/oneagent/agent/processagent(/.*)?	unconfined_u:object_r:lib_t:s0
/etc/selinux/targeted/active/file_contexts:/opt/dynatrace/oneagent/agent/lib64/oneagentdynamizer	unconfined_u:object_r:oneagent_exec_t:s0
/etc/selinux/targeted/active/file_contexts:/opt/dynatrace/oneagent/agent/libmusl64/oneagentdynamizer	unconfined_u:object_r:oneagent_exec_t:s0
```

On the firewall side,

DynaTrace managed setup, the most commonly used ports are:

- Port 443: used for HTTPS communication with the DynaTrace cluster and for DynaTrace Agents to communicate with the cluster
- Port 9999: used for DynaTrace Agents to communicate with the cluster for certain protocols such as JMX and .NET remoting
- Port 8020: used for communication between DynaTrace Server and Cassandra database nodes

However, it's worth noting that there may be other ports in use depending on your specific DynaTrace deployment configuration.

Post install you might want to check with the following command,

```
# iptables -L --numeric
Chain INPUT (policy ACCEPT)
target     prot opt source               destination         
ACCEPT     all  --  0.0.0.0/0            0.0.0.0/0            state RELATED,ESTABLISHED
ACCEPT     tcp  --  0.0.0.0/0            0.0.0.0/0            tcp multiport ports 443,8021:8022,8443
ACCEPT     tcp  --  192.168.1.12         0.0.0.0/0            tcp multiport ports 5701:5711,7000:7001,8019:8020,9042,9200,9300,9998
ACCEPT     tcp  --  0.0.0.0/0            0.0.0.0/0            ADDRTYPE match src-type LOCAL tcp multiport ports 443,5701:5711,7000:7001,7199,8018:8022,8443,9042,9200,9300,9998
DROP       tcp  --  0.0.0.0/0            0.0.0.0/0            tcp multiport ports 443,5701:5711,7000:7001,7199,8018:8022,8443,9042,9200,9300,9998

Chain FORWARD (policy ACCEPT)
target     prot opt source               destination         
ACCEPT     all  --  0.0.0.0/0            0.0.0.0/0            state RELATED,ESTABLISHED

Chain OUTPUT (policy ACCEPT)
target     prot opt source               destination         
ACCEPT     tcp  --  0.0.0.0/0            0.0.0.0/0            tcp multiport ports 443,5701:5711,7000:7001,8019:8022,8443,9042,9200,9300,9998
```

Record what you find incase policy or firewall changes step on those.

Blog tags

[linux](/taxonomy/term/7)

[Dynatrace](/taxonomy/term/17)

[Dynatrace Managed](/taxonomy/term/42)

Submitted by auzieman
 on Fri, 03/17/2023 - 17:41

