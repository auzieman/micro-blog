---
title: "A practical opensource puppet setup."
slug: "a-practical-opensource-puppet-setup"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/14"
source_id: "node-14"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Feeling fed up firefighting? wish you had a sensible way to keep core packages and configurations in place across your network? Not ready for expensive config management tools?

Maybe you should explore Puppet from Puppet Labs. If you want commercial support and even more features Puppet Labs has that too!

Sure you could script something together maybe even make it robust and extensible. However we in IT do this all to often, for nearly anything we can think of there probably already is a program for that.  So rather than reinventing yet another wheel..

Consider this example,

Keep in mind I’ve sifted through a number of other blogs and postings around this topic.  For me on multiple machines, multiple OS’s this was the most consistent sequence of steps.  Also this outline covers a reasonable selection of the most useful features Puppet has for us Admins, this outline could be extended dramatically and certainly doesn’t cover everything.

Requirements,

 Valid dns entries for all hosts, see rapid dns/dhcp setup for lans.  For CentOS hosts as noted rpmforge repository is required.  For Ubuntu and several other distributions the package should be available by default.

Puppet master;

CentOS 5.7 + additional repositories RPMForge and EPEL

```
yum install puppet-server
```

On the nodes (optionally on the master too)

```
yum install puppet
service puppet restart
```

Adjust the default config file, /etc/puppet/puppet.conf

```
[main]
logdir=/var/log/puppet
vardir=/var/lib/puppet
ssldir=/var/lib/puppet/ssl
rundir=/var/run/puppet
factpath=$vardir/lib/facter
templatedir=$confdir/templates
prerun_command=/etc/puppet/etckeeper-commit-pre
postrun_command=/etc/puppet/etckeeper-commit-post
server=puppet.yourdomain.lan

[master]
# These are needed when the puppetmaster is run by passenger
# and can safely be removed if webrick is used.
ssl_client_header = SSL_CLIENT_S_DN
ssl_client_verify_header = SSL_CLIENT_VERIFY
```

Manually restart the service,

```
service puppet restart
```

Manually verify conectivity,

```
puppetd -vt –debug –server puppet.yourdomain.lan
```

Optionally you can skip to the end and sign your nodes now but I’d suggest continuing on.  Of course you may have issues with things like firewall rules etc.  

Sample /etc/puppet files for puppet master,

```
Contents of /etc/puppet/auth.conf

path ~ ^/catalog/([^/]+)$
method find
allow $1

path ~ ^/node/([^/]+)$
method find
allow $1

path /certificate_revocation_list/ca
method find
allow *

path /report
method save
allow *

path /file
allow *

path /certificate/ca
auth no
method find
allow *

path /certificate/
auth no
method find
allow *

path /certificate_request
auth no
method find, save
allow *

path /
auth any

 Contents of /etc/puppet/fileserver.conf

[files]
  path /etc/puppet/files
  allow *.yourdomain.lan

[plugins]
 allow *.yourdomain.lan

Contents of /etc/puppet/puppet.conf

[main]
    # The Puppet log directory.
    # The default value is ‘$vardir/log’.
    logdir = /var/log/puppet

    # Where Puppet PID files are kept.
    # The default value is ‘$vardir/run’.
    rundir = /var/run/puppet

    # Where SSL certificates are kept.
    # The default value is ‘$confdir/ssl’.
    ssldir = $vardir/ssl

[agent]
    # The file in which puppetd stores a list of the classes
    # associated with the retrieved configuratiion.  Can be loaded in
    # the separate “puppet“ executable using the “–loadclasses“
    # option.
    # The default value is ‘$confdir/classes.txt’.
    classfile = $vardir/classes.txt

    # Where puppetd caches the local configuration.  An
    # extension indicating the cache format is added automatically.
    # The default value is ‘$confdir/localconfig’.
    localconfig = $vardir/localconfig

Suggested files,

]# ls /etc/puppet/manifests/
classes/  nodes.pp  site.pp

 Contents of /etc/puppet/manifests/site.pp

import “classes/*”
import “nodes”

Contents of /etc/puppet/manifests/nodes.pp

node workstation {
 include autofs
 include hostsfile
 include nis
 include specialuser
}

node servers {
 include ntp specialuser hostsfile
}

node ‘testhost1.yourdomain.lan’ inherits workstation {
}
node ‘testhost2.yourdomain.lan’ inherits workstation {
}

Define some classes,
 Here I created the following files which are cited by their main file name above in the node examples.

]# ls /etc/puppet/manifests/classes/
autofs.pp    hosts.pp       nis.pp         specialuser.pp      

Contents of  /etc/puppet/manifests/classes/autofs.pp

class autofs {
 package{‘autofs’:
    name => ‘sutofs’,
    ensure => present
 }
}

 Contents of  /etc/puppet/manifests/classes/hosts.pp

class hostsfile {
    file { “/etc/hosts.puppet”:
        owner   => root,
        group   => root,
        mode    => 775,
        source  => “puppet:///hosts/hosts”
        #Special hosts file would live here on puppet master /etc/puppet/files/hosts/hosts
    }
}

Contents of  /etc/puppet/manifests/classes/nis.pp

class nis {
 # determine the apache-server package based on the operating system fact
    $nis = $operatingsystem ? {
        centos => “ypbind”,
        ubuntu => “nis”,
    }
 
    package { “$nis”:
        ensure => “present”,
        alias  => “nis”,
    }
}

Contents of  /etc/puppet/manifests/classes/specialuser.pp

class stokeadmin {
 user {‘specialuser’:
    ensure => ‘present’,
    home => ‘/usr/local/specialuser’,
    shell => ‘/bin/bash’,
    password => ‘specialpass’,
    uid => ‘1000’,
    gid => ‘1000’,
    groups => [‘adm’,’dialout’,’cdrom’,’sudo’,’plugdev’,’lpadmin’,’admin’,’sambashare’]
}
}
```

Now with all this in place

restart puppet master service,

```
/etc/init.d/puppetmaster restart
```

or,

```
service puppetmaster restart
```

Given the two hosts exist and have dns entries as noted above on each host restart puppet.

```
/etc/init.d/puppet restart
```

or,

```
service puppet restart
```

On the puppet master,

```
puppetca –list

puppetca –sign testhost1.yourdomain.lan

puppetca –sign testhost2.yourdomain.lan

puppetca –list –all
+ testhost1.yourdomain.lan (21:4A:1C:5A:85:52:E4:BA:32:A6:1B:BC:14:0F:63:B1)
+ testhost2.yourdomain.lan (55:7E:C1:B1:CE:0B:0C:F2:9F:F4:AE:E0:95:15:2A:F3)
+ puppet.yourdomain.lan (47:0F:83:CD:C8:9B:C7:96:16:A8:A2:89:13:49:7D:EC)
```

Once this has all been done you should shortly see the above noted changes on the test hosts.  You should also be able to forcibly remove or alter the changes and see them be reverted after about 15 min if not sooner.

# Post navigation

Blog tags

[puppet](/taxonomy/term/9)

[automation](/taxonomy/term/10)

[linux](/taxonomy/term/7)

Submitted by auzieman
 on Wed, 07/20/2022 - 09:44

