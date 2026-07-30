---
title: A Practical Puppet Setup for Small Linux Environments
slug: practical-puppet-setup-small-linux-environments
summary: A restored practical Puppet walkthrough for small Linux environments: master, agents, DNS, packages, manifests, modules, files, and service drift.
tags: [linux, puppet, configuration-management, teaching, operations]
theme_variant: linux-pro
status: published
seo_title: Practical Puppet setup for small Linux environments
seo_description: A grounded Puppet-style configuration management lesson for small Linux environments and repeatable operations.
---

Configuration management is most useful when it makes boring work stay boring.

Puppet, Ansible, shell templates, and BlackKnightController pipelines all circle
the same operator problem: a server should not depend on one person remembering
every package, config file, service restart, and permission tweak.

The older Auzietek article was written from the “fed up firefighting” side of
operations. That voice still matters. If you are constantly logging into
machines to repair packages, users, service flags, or config files by hand, the
environment is asking for a declarative model.

Puppet's lesson is still valuable:

```text
describe the desired state
apply it repeatably
notice drift
repair drift without drama
```

## Start with boring prerequisites

Do not begin with clever modules. Begin with the boring items that make
configuration management reliable:

- working DNS or explicit host records for every managed machine;
- time synchronization;
- repositories enabled for the distributions in the lab;
- a reachable Puppet master;
- a small number of test nodes;
- a clear answer for which files automation owns.

The original notes used CentOS-era examples. The exact packages may differ on
modern distributions, but the shape remains useful.

On the master:

```bash
yum install puppet-server
service puppetmaster start
chkconfig puppetmaster on
```

On each node:

```bash
yum install puppet
service puppet start
chkconfig puppet on
```

On Debian-family systems the package/service names may differ, but the idea is
the same: install the agent, point it at the master, and make the service
repeatable across reboot.

## Certificate handshake

Puppet uses a trust relationship between the master and each agent. The node
connects, presents a certificate request, and the master signs it.

Useful operator commands:

```bash
puppet cert list
puppet cert sign node1.example.com
puppet agent --test
```

The certificate step is easy to skip in a short tutorial, but in real labs it is
one of the first places a deployment stalls. DNS names, hostnames, and cert
names need to agree.

## Start with one service pattern

For a small Linux environment, do not begin with a giant “manage everything”
module. Start with one service pattern:

```text
install the package
place the config file
enable and start the service
expose the port intentionally
validate the result
```

That sequence fits almost every infrastructure tool.

Here is the Puppet mental model in miniature:

```puppet
package { 'httpd':
  ensure => installed,
}

file { '/etc/httpd/conf.d/example.conf':
  ensure  => file,
  owner   => 'root',
  group   => 'root',
  mode    => '0644',
  source  => 'puppet:///modules/example/example.conf',
  require => Package['httpd'],
  notify  => Service['httpd'],
}

service { 'httpd':
  ensure    => running,
  enable    => true,
  subscribe => File['/etc/httpd/conf.d/example.conf'],
}
```

That small example contains most of the lesson:

- package state comes before file state;
- file ownership is explicit;
- config changes notify the service;
- service state survives reboot;
- the operator can read the intent without guessing.

## Keep modules small and obvious

A readable module or role should answer simple questions:

- what does this machine need installed?
- which files are owned by automation?
- what should restart when a file changes?
- which defaults are safe?
- where should local overrides live?

If the answer is hidden in clever conditionals, the system may be automated but
not understandable.

The first useful module layout can be plain:

```text
modules/
  example/
    manifests/
      init.pp
    files/
      example.conf
    templates/
      example.conf.erb
```

Templates are where Puppet becomes more than file copying. A template lets you
use a common shape while still adjusting host-specific values:

```erb
ServerName <%= @fqdn %>
Listen <%= @listen_port %>
```

That same idea shows up in BKC pipelines when scripts, configs, and installer
answers are rendered from structured fragments before being shipped to a host.

## Caveats from the real world

Puppet is powerful, but it is not magic:

- bad DNS makes cert enrollment painful;
- too many conditionals make manifests hard to reason about;
- unmanaged local edits will be overwritten if Puppet owns the file;
- service restarts need to be intentional;
- cross-distro modules require discipline around package and service names.

Those caveats are not reasons to avoid configuration management. They are the
reason to keep the first modules boring and documented.

## The BKC connection

BlackKnightController does not need to become Puppet to learn from Puppet.

The useful fragment is the operating shape: capture known-good steps, keep
parameters explicit, validate the end state, and preserve enough evidence that
the next operator understands what happened.

That is the shared lesson across configuration management generations.

Legacy source: [A practical opensource Puppet setup](https://auzietek.com/node/14).
