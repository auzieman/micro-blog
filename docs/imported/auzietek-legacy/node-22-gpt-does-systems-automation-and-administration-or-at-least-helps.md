---
title: "GPT Does systems automation and administration, or at least helps"
slug: "gpt-does-systems-automation-and-administration-or-at-least-helps"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/22"
source_id: "node-22"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

So, it can code, it can respond to human questions and provide human replies or translate human to code.  Can it help or even administer and automate servers and workstations? This was a simple test of its ability to craft an Ansible playbook for a hypothetical Linux workstation.

Obviously just a seed not a full solution.

ME:

With Ansible group vars maintain two users, root and joe_user each with password of updateme, joe_user should have passwordless sudo also.  Then the playbook should also ensure gedit is installed as well as all updates. The hosts inventory should be workstation1,2 and three each in the group that gets the users and updates. Lastly the machines are all running Fedora 35.

Here is GPT's initial response

```
---

- hosts: workstations
  vars:
    root_pw: updateme
    joe_user_pw: updateme
    joe_user_sudo: True
  tasks:
    - name: Install gedit
      package: name=gedit state=present
    - name: Create root user
      user: name=root password={{ root_pw }}
      become: yes
    - name: Create joe_user
      user: name=joe_user password={{ joe_user_pw }}
      become: yes
    - name: Give joe_user sudo access
      become: yes
      lineinfile:
        path: /etc/sudoers
        state: present
        line: 'joe_user ALL=(ALL) NOPASSWD:ALL'
      when: joe_user_sudo
    - name: Update Fedora
      package: name=* state=latest
      become: yes
```

While the example is simple, it got the basics dead right.  Later I tested having it make improvements to this and it was rather effective there too.  Right now its offline, but I plan to come back at it with this and ask to translate to master-less puppet and perhaps a Fabric equivalent as well.

ME:

With masterless puppet including a local hiera maintain two users, root and joe_user each with password of updateme, joe_user should have passwordless sudo also.  Then the playbook should also ensure gedit is installed as well as all updates. The hosts inventory should be workstation1,2 and three each in the group that gets the users and updates. Lastly the machines are all running Fedora 35.

GPT:

To accomplish this, you can use Ansible to create a playbook that uses Masterless Puppet and Hiera to manage the users and their passwords, as well as ensure that gedit is installed and all updates are applied. Here's an example playbook:

```
- hosts: workstation1, workstation2, workstation3
  become: true

  tasks:
    # Install puppet and hiera
    - name: Install Puppet and Hiera
      package:
        name: "{{ item }}"
        state: present
      with_items:
        - puppet
        - hiera

    # Set up Hiera data
    - name: Set up Hiera data
      copy:
        src: hiera.yaml
        dest: /etc/puppetlabs/puppet/hiera.yaml
      become: true

    # Apply puppet manifests using Hiera
    - name: Apply Puppet manifests
      command: puppet apply /etc/puppetlabs/code/environments/production/manifests/ --hiera_config /etc/puppetlabs/puppet/hiera.yaml
      become: true

    # Install gedit
    - name: Install gedit
      package:
        name: gedit
        state: present

    # Apply system updates
    - name: Apply system updates
      package:
        name: "*"
        state: latest
        update_cache: yes

    # Set up users and their passwords
    - name: Set up root user
      user:
        name: root
        password: "{{ 'updateme' | password_hash('sha512') }}"
      become: true

    - name: Set up joe_user
      user:
        name: joe_user
        password: "{{ 'updateme' | password_hash('sha512') }}"
      become: true

    # Allow joe_user passwordless sudo
    - name: Allow joe_user passwordless sudo
      lineinfile:
        path: /etc/sudoers
        line: "joe_user ALL=(ALL) NOPASSWD: ALL"
        state: present
        validate: 'visudo -cf %s'
```

Make sure to create a Hiera configuration file (hiera.yaml) in the directory /etc/puppetlabs/puppet/ with the following content:

```
makefile

version: 5

hierarchy:
  - name: "Users"
    path: "users.yaml"
```

Create a data file for users (/etc/puppetlabs/code/environments/production/data/users.yaml) with the following content:

```
---
users:
  - name: root
    password: updateme
  - name: joe_user
    password: updateme
```

This playbook assumes that you have already installed Ansible on your local machine and have a working inventory file that lists the hosts in the workstation1, workstation2, and workstation3 groups. Also, the playbook assumes that your hosts are already running Fedora 35.

To run the playbook, save it as a file (e.g., workstation-setup.yml) and execute the following command:

```
ansible-playbook -i inventory.ini workstation-setup.yml
```

where inventory.ini is the path to your inventory file.

ME:

Humm that seems a little off, I agree with the approach of getting puppet there but shouldn't the packaging and versioning all be handled by puppet instead of ansible? What if Ansible ensures puppet and hiera and the various puppet files are in place but puppet handles the users, packages and updates?

GPT:

You're correct, it would be better to have Puppet handle the package installation, user management, and updates, rather than Ansible. Here's an updated playbook that assumes that Puppet is already installed on the target hosts and that the Puppet files are stored in the /etc/puppetlabs/code/environments/production directory.

```
- hosts: workstation1, workstation2, workstation3
  become: true

  tasks:
    # Set up Hiera data
    - name: Set up Hiera data
      copy:
        src: hiera.yaml
        dest: /etc/puppetlabs/puppet/hiera.yaml
      become: true

    # Apply puppet manifests using Hiera
    - name: Apply Puppet manifests
      command: puppet apply /etc/puppetlabs/code/environments/production/manifests/ --hiera_config /etc/puppetlabs/puppet/hiera.yaml
      become: true
```

In this playbook, we're using Ansible to copy the hiera.yaml file to the target hosts and then we're executing puppet apply command to apply the Puppet manifests using Hiera.

Here's an example Puppet manifest (e.g., site.pp) that defines the users and packages that should be installed and updated:

```
# Define users
user { 'root':
  password => '$6$g08c/6cqYAViFnHr$VmL9XjKqtXDzNztNnZZ2bsN8GzL1S31tS5vIylQeSQZ9XO/J/q4V4jHlStOxVd17u.BkzqV7dIGtebWjZ8Xla0',
}

user { 'joe_user':
  password => '$6$g08c/6cqYAViFnHr$VmL9XjKqtXDzNztNnZZ2bsN8GzL1S31tS5vIylQeSQZ9XO/J/q4V4jHlStOxVd17u.BkzqV7dIGtebWjZ8Xla0',
}

# Allow joe_user passwordless sudo
sudo::conf { 'joe_user':
  content => 'joe_user ALL=(ALL) NOPASSWD: ALL',
}

# Install gedit package
package { 'gedit':
  ensure => 'installed',
}

# Apply system updates
package { 'updates':
  ensure => 'latest',
}
```

In this manifest, we're using the user, sudo::conf, and package resources to define the root and joe_user users, allow joe_user passwordless sudo, install the gedit package, and apply system updates.

Make sure to create a Hiera configuration file (hiera.yaml) in the directory /etc/puppetlabs/puppet/ with the following content:

```
makefile
version: 5

hierarchy:
  - name: "Users"
    path: "users.yaml"
```

Create a data file for users (/etc/puppetlabs/code/environments/production/data/users.yaml) with the following content:

```
---
users:
  - name: root
    password: updateme
  - name: joe_user
    password: updateme
```

Finally, make sure to include the sudo module in your Puppet environment by adding the following to your Puppetfile:

```
mod 'puppetlabs-sudo'
```

Once the Puppet manifests and Hiera data are in place, you can execute the Ansible playbook to apply the Puppet configuration:

And we hit the limit..

ME:

Ok perty good thinking there, in the past I put something simliar together using XLDeploy to deliver the puppet files over ssh, trigger the yum (dnf) installs if needed. Admittedly that was not workstation setup but rather other administrative tasks. So I'm thinking Ansible could replace ssh, trigger if needed puppet and hira to be installed, deliver the required files then finally trigger the puppet apply.

GPT:

Yes, that's a good approach as well. Using Ansible to deliver the Puppet files, install Puppet and Hiera, and then trigger puppet apply can help automate the entire process.

Here's an updated playbook that assumes that Puppet and Hiera are not installed on the target hosts, and that Ansible is being used to install them, deliver the required Puppet files, and trigger puppet apply:

```
- hosts: workstation1, workstation2, workstation3
  become: true

  tasks:
    # Install Puppet and Hiera
    - name: Install Puppet and Hiera
      dnf:
        name: "{{ item }}"
        state: present
      loop:
        - puppet-agent
        - puppetserver
        - hiera

    # Set up Puppet environment
    - name: Set up Puppet environment
      copy:
        src: puppet.tar.gz
        dest: /tmp/puppet.tar.gz
      become: true

    - name: Extract Puppet environment
      unarchive:
        src: /tmp/puppet.tar.gz
        dest: /etc/puppetlabs/code/environments/
        remote_src: yes
      become: true

    # Apply Puppet manifests using Hiera
    - name: Apply Puppet manifests
      command: puppet apply /etc/puppetlabs/code/environments/production/manifests/ --hiera_config /etc/puppetlabs/puppet/hiera.yaml
      become: true
```

In this playbook, we're using Ansible's dnf module to install Puppet and Hiera, copying a tarball containing the Puppet files to the target hosts, extracting it to the correct location, and then executing puppet apply to apply the Puppet manifests.

Make sure to create a tarball containing the Puppet files (e.g., puppet.tar.gz) that includes the necessary Puppet modules and manifests.

Also, note that this playbook assumes that the target hosts are running Fedora 35, which uses dnf instead of yum for package management. If you're running an older version of Fedora or a different distribution, you may need to adjust the package manager used in the playbook.

ME:

wow, I'm going to add this to my post of your ansible version, very interesting results mr gpt :)

GPT:

Glad to hear that you found this helpful! Feel free to let me know if you have any other questions or need further assistance.

.... ..

Soo a little hit and miss on this one but still staggering I must say, this technology is going to really change the world.  More and more I wonder if we are all ready for it?

Blog tags

[openAI](/taxonomy/term/27)

[ChatGPT](/taxonomy/term/23)

[ansible](/taxonomy/term/12)

[linux](/taxonomy/term/7)

[sudoers](/taxonomy/term/31)

[puppet](/taxonomy/term/9)

[shell scripting](/taxonomy/term/32)

Submitted by auzieman
 on Wed, 02/15/2023 - 16:59

