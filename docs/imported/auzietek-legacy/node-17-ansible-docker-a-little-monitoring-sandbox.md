---
title: "Ansible, Docker, a little monitoring sandbox"
slug: "ansible-docker-a-little-monitoring-sandbox"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/17"
source_id: "node-17"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Not to be too complex, but lets say you want to checkout OpenTSDB, Grafana, Docker, Ansible all in one sitting.

Preparation,

This snippet of prep.yaml needs more work but hits most of the prerequisites.

```
---
  - hosts: all
    become: true
    vars:
      auto_create_metrics: true
      cors_domains: "*"
    tasks:
      - name: Creates directories
        file:
            path: "{{ item }}"
            state: directory
        with_items:
          - /srv/grafana/grafana_data
          - /srv/grafana/grafana_provisioning
          - /srv/mysqlgrafana
      - name: This command will will pull mysql
        ansible.builtin.shell:
          cmd: docker pull mysql:5.7
      - name: This command will will pull and run a named copy of grafana
        ansible.builtin.shell:
          cmd: docker run -td --name grafana grafana/grafana-oss
      - name: This command will will pull and run a named copy of OpenTSB
        ansible.builtin.shell:
          cmd: docker run -td --name opentsdb petergrace/opentsdb-docker
      - name: This command will populate  the /srv directory
        ansible.builtin.shell:
          cmd: docker cp grafana:/var/lib/grafana /srv/grafana/grafana_data
      - name: This command will populate  the /srv with grafana data
        ansible.builtin.shell:
          cmd: docker cp grafana:/etc/grafana/provisioning /srv/grafana/grafana_provisioning
      - name: This command will populate  the /srv with opentsdb data
        ansible.builtin.shell:
          cmd: docker cp opentsdb:/usr/local/share/opentsdb/etc/opentsdb /srv/
      - name: This command will make the monitoring network
        ansible.builtin.shell:
          cmd: docker network create monitoring
      - name: Install telegraf
        ansible.builtin.package:
          name: telegraf
          state: present
      - name: This command will shut down grafana
        ansible.builtin.shell:
          cmd: docker kill grafana
      - name: This command will shut down opentsdb
        ansible.builtin.shell:
          cmd: docker kill opentsdb
```

Telegraf for host and docker statistics.

```
[global_tags]
[agent]
  interval = "10s"
  round_interval = true
  metric_batch_size = 1000
  metric_buffer_limit = 10000
  collection_jitter = "0s"
  flush_interval = "10s"
  flush_jitter = "0s"
  precision = "0s"
  hostname = ""
  omit_hostname = false

[[outputs.opentsdb]]
prefix = "telegraf."
host = "localhost"
port = 4242

[[inputs.cpu]]
  percpu = true
  totalcpu = true
  collect_cpu_time = false
  report_active = false
  core_tags = false

[[inputs.disk]]
  ignore_fs = ["tmpfs", "devtmpfs", "devfs", "iso9660", "overlay", "aufs", "squashfs"]

[[inputs.diskio]]

[[inputs.kernel]]

[[inputs.mem]]

[[inputs.processes]]

[[inputs.swap]]

[[inputs.system]]

# # Read metrics about docker containers
[[inputs.docker]]
endpoint = "unix:///var/run/docker.sock"

[[inputs.ethtool]]
interface_exclude = ["enp0s29u1u5","br-1978850bc091","docker0"]

[[inputs.net]]
interfaces = ["eth*", "enp0s29u1u5", "lo"]

[[inputs.netstat]]

[[inputs.ping]]
urls = ["www.google.com"]

[[inputs.exec]]
commands = ["/usr/bin/speedtest -f json-pretty"]
name_override = "Speedtest"
timeout = "1m"
interval = "5m"
data_format = "json"
json_string_fields = [ "interface_externalIp",
                       "server_name",
                       "server_location",
                       "server_host",
                       "server_ip",
                       "result_url" ]

[[inputs.procstat]]
pattern = ".*"
```

Playbook.yaml

```
---
  - hosts: all
    tasks:
      - name: MySQLContainer
        docker_container:
          name: mysql
          image: mysql:5.7
          state: stopped
          ports:
            - "3366:3306"
          volumes:
            - "/srv/mysqlgrafana:/var/lib/mysql:rw"
          networks:
            - name: pick-one-make-one
          env:
            MYSQL_ROOT_PASSWORD: changethis
            MYSQL_DATABASE: grafana
            MYSQL_USER: grafana
            MYSQL_PASSWORD: changethis
      - name: opentsdb service
        docker_container:
          name: opentsdb
          image: petergrace/opentsdb-docker
          state: stopped
          networks:
            - name: pick-one-make-one
          ports:
            - "4242:4242"
            - "60030:60030"
          volumes:
            - "/srv/opentsb/opentsdb:/usr/local/share/opentsdb/etc/opentsdb:rw"
      - name: Grafana service
        docker_container:
          name: grafana
          image: grafana/grafana-oss
          state: stopped
          networks:
            - name: pick-one-make-one
          ports:
            - "3000:3000"
          env:
              MYSQL_ROOT_PASSWORD: changethis
              MYSQL_DATABASE: grafana
              MYSQL_USER: grafana
              MYSQL_PASSWORD: changethis
```

Inventory.yaml

```
all:
  hosts:
    127.0.0.1:
      ansible_connection: local
```

Blog tags

[docker](/taxonomy/term/11)

[ansible](/taxonomy/term/12)

[linux](/taxonomy/term/7)

[grafana](/taxonomy/term/13)

[mysql](/taxonomy/term/14)

[opentsdb](/taxonomy/term/15)

[telegraf](/taxonomy/term/16)

Submitted by auzieman
 on Tue, 09/06/2022 - 15:56

