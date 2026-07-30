---
title: "Open Telemetry ansible and dynatrace :WIP"
slug: "open-telemetry-ansible-and-dynatrace-wip"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/19"
source_id: "node-19"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
  - source: "embedded-data-uri"
    status: "skipped"
    local: ""
---

In this little post I'm going to take you through a somewhat rare topic, Open Telemetry and Dynatrace. As time allows I'll clean this up a bit more and add a collector container etc. For now its at a shareable and repeatable state.

For now the following playbook is uploaded to the target and adjusted for both the host and Dynatrace account. a prepatory network create in docker is also needed.

```
$ docker network create otel
$ vi Project.yaml
edit the vars for your setup,
        env:
          API_HOST: replace.live.dynatrace.com
          API_TOKEN: replace
Optionally modify the port mapping in otel_test.py and the project yaml.
        ports:
          - "8080:8080"
$ vi otel_test/otel_test.py
                      "content": f'{json.dumps({"latency_info":[{"task":"Start","started":"0"},{"task":"api-routing","started":"0"},{"task":"api-cors","started":"4"},{"task":"api-client-identification","started":"4"},{"task":"assembly-ratelimit","started":"4"},{"task":"api-security","started":"4"},{"task":"assembly-gatewayscript","started":"4"},{"task":"assembly-xslt","started":"5"},{"task":"assembly-switch","started":"5"},{"task":"assembly-set-variable","started":"5"},{"task":"assembly-switch","started":"5"},{"task":"assembly-function-call","started":"5"},{"task":"api-execute","started":"5"},{"task":"assembly-invoke","started":"6"},{"task":"assembly-switch","started":"32"},{"task":"assembly-function-call","started":"32"}]})}'}
        try:
            r = requests.get('http://otel_server:8080', headers=headers, timeout=5)
            if error != 0:
```

Now I actually have all this and various scraps and ideas in my dtlabs git repo, https://dtlab.auzietek.com/auzieman/OTEL_Pythondemo

However lets jump through some of the key files,

Generate an open telemetry access token for the setup.

[embedded image skipped: data URI was too large for staged Markdown]

The client tests the server and sends its own metrics, traces etc. later I'll stub in a metric and possibly event example here too.

```
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
import requests
import random
import time
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

from datetime import datetime
import json
import os

api_host = os.environ.get('API_HOST', 'guu84124.live.dynatrace.com')
api_token = os.environ.get('API_TOKEN', 'asdfasdfasdfasdfaasdf')

span_exporter = OTLPSpanExporter(
    endpoint="https://"+str(api_host)+"/api/v2/otlp/v1/traces", #TODO replace <URL> with the URL as determined in section 2 above
    headers={
        "Authorization": "Api-Token "+str(api_token) #TODO replace <TOKEN> with the authentication token created in section 2 above
    },
)

resource = Resource.create({
    # customizable resource attributes
    "service.name": "otel_python_test_client",
    "service.version": "1.0.0"
})

trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer_provider().get_tracer(__name__)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(span_exporter)
)
trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(ConsoleSpanExporter())
)

def main():
    ERROR = False
    with tracer.start_as_current_span("Incoming Request", kind=trace.SpanKind(1)) as parent_span:
     with tracer.start_as_current_span("API Connect Gateway Request", kind=trace.SpanKind(2)) as span:
        # trace_parent = f"00-{trace.format_trace_id(span.get_span_context().trace_id)}-{trace.format_span_id(span.get_span_context().span_id)}-01"
        error = random.randint(0,4)
        headers = {"Content-Type":"application/json"}
        TraceContextTextMapPropagator().inject(headers)
        print(trace.format_trace_id(span.get_span_context().trace_id))
        print(trace.format_span_id(span.get_span_context().span_id))
        log_message = {"trace_id": str(trace.format_trace_id(span.get_span_context().trace_id)),
                       "span_id" : str(trace.format_span_id(span.get_span_context().span_id)),
                       "content": f'{json.dumps({"latency_info":[{"task":"Start","started":"0"},{"task":"api-routing","started":"0"},{"task":"api-cors","started":"4"},{"task":"api-client-identification","started":"4"},{"task":"assembly-ratelimit","started":"4"},{"task":"api-security","started":"4"},{"task":"assembly-gatewayscript","started":"4"},{"task":"assembly-xslt","started":"5"},{"task":"assembly-switch","started":"5"},{"task":"assembly-set-variable","started":"5"},{"task":"assembly-switch","started":"5"},{"task":"assembly-function-call","started":"5"},{"task":"api-execute","started":"5"},{"task":"assembly-invoke","started":"6"},{"task":"assembly-switch","started":"32"},{"task":"assembly-function-call","started":"32"}]})}'}
        try:
            r = requests.get('http://otel_server:8080', headers=headers, timeout=5)
            if error != 0:
                print("No Error")
                span.set_status(status=trace.StatusCode(1))
            else:
                print("Error")
                span.set_status(status=trace.StatusCode(2), description=str("Simulated Error"))
                ERROR = True
        except Exception as e:
            span.set_status(trace.Status(status_code=trace.StatusCode(2), description=str(e)))
            print(e)
            ERROR = True
        span.set_status(status=trace.StatusCode(1))
    if ERROR:
        parent_span.set_status(status=trace.StatusCode(2))
    else:
        parent_span.set_status(status=trace.StatusCode(1))

if __name__ == '__main__':
    while True:
        time.sleep(random.randint(0,5))
        main()
```

The server is just a simple running process also emitting metrics, traces etc

```
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
    OTLPSpanExporter,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
import os

api_host = os.environ.get('API_HOST', 'guu84124.live.dynatrace.com')
api_token = os.environ.get('API_TOKEN', 'asdfasdfasdfasdfaasdf')

span_exporter = OTLPSpanExporter(
    endpoint="https://"+str(api_host)+"/api/v2/otlp/v1/traces", #TODO replace <URL> with the URL as determined in section 2 above
    headers={
        "Authorization": "Api-Token "+str(api_token) #TODO replace <TOKEN> with the authentication token created in section 2 above
    },
)

resource = Resource.create({
    #customizable resource attributes
    "service.name": "otel_python_test_server",
    "service.app_id": "42",
    "service.version": "1.0.0"
})

trace.set_tracer_provider(TracerProvider(resource=resource))
tracer = trace.get_tracer_provider().get_tracer(__name__)

trace.get_tracer_provider().add_span_processor(
    BatchSpanProcessor(span_exporter)
)

from http.server import BaseHTTPRequestHandler, HTTPServer
import time

hostName = "otel_server"
serverPort = 8080

class MyServer(BaseHTTPRequestHandler):
    def do_GET(self):
        ctx = TraceContextTextMapPropagator().extract(carrier=self.headers)

        with tracer.start_as_current_span("HTTP GET", context=ctx, kind=trace.SpanKind(1)) as span:
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()

if __name__ == "__main__":
    webServer = HTTPServer((hostName, serverPort), MyServer)
    print("Server started http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
```

There are some intended random faults worked in also, this all gets built by a couple of Dockerfiles like this one,

```
FROM python:3.10
# Or any preferred Python version.# Or any preferred Python version.
RUN pip install --upgrade pip
RUN apt-get -y update
RUN apt-get -y upgrade
RUN pip install opentelemetry-api pip install opentelemetry-exporter-otlp boto3
ADD otel_server.py .
RUN pip install requests beautifulsoup4 python-dotenv
USER root
CMD python /otel_server.py
# Or enter the name of your unique directory and parameter set.
```

.The trigger as mentioned earlier to spawn it all is a simple ansible playbook, you will need to token and your dynatrace tenant information as well as possibly editing the external port used.

```
- hosts: all
  tasks:
    - name: build otel_test container image
      docker_image:
        name: otel_test:v0.01
        build:
          path: ./otel_test
        state: present
    - name: build otel_server container image
      docker_image:
        name: otel_server:v0.01
        build:
          path: ./otel_server
        state: present
    - name: otel_server container
      docker_container:
        name: otel_server
        image: otel_server:v0.01
        state: started
        restart_policy: always
        ports:
          - "8080:8080"
        networks:
          - name: otel
        env:
          API_HOST: replace.live.dynatrace.com
          API_TOKEN: replace
    - name: otel_test container
      docker_container:
        name: otel_test
        image: otel_test:v0.01
        state: started
        restart_policy: always
        networks:
          - name: otel
        env:
          API_HOST: replace.live.dynatrace.com
          API_TOKEN: replace
```

Finally the running of the playbook needs an inventory I've only ran this locally so far, partially due to not wanting the image build to get copied to the remote target, so this simple example should be fine.  Keep in mind the target needs ansible, python docker and of course python3 is best.

```
all:
  hosts:
    127.0.0.1:
      ansible_connection: local
      ansible_python_interpreter: /usr/bin/python3
```

And now the run and the results,

```
$ ansible-playbook -i Inventory-local.yaml Project.yaml
PLAY [all] *********************************************************************

TASK [Gathering Facts] *********************************************************
ok: [127.0.0.1]

TASK [build otel_test container image] *****************************************
[WARNING]: The value of the "source" option was determined to be "build".
Please set the "source" option explicitly. Autodetection will be removed in
Ansible 2.12.
ok: [127.0.0.1]

TASK [build otel_server container image] ***************************************
ok: [127.0.0.1]

TASK [otel_server container] ***************************************************
[DEPRECATION WARNING]: Please note that docker_container handles networks
slightly different than docker CLI. If you specify networks, the default
network will still be attached as the first network. (You can specify
purge_networks to remove all networks not explicitly listed.) This behavior
will change in Ansible 2.12. You can change the behavior now by setting the new
 `networks_cli_compatible` option to `yes`, and remove this warning by setting
it to `no`. This feature will be removed in version 2.12. Deprecation warnings
can be disabled by setting deprecation_warnings=False in ansible.cfg.
changed: [127.0.0.1]

TASK [otel_test container] *****************************************************
changed: [127.0.0.1]

PLAY RECAP *********************************************************************
127.0.0.1                  : ok=5    changed=2    unreachable=0    failed=0    skipped=0    rescued=0    ignored=0
```

If everything goes well on the target docker instance dynatrace should show the new containers,

[embedded image skipped: data URI was too large for staged Markdown]

In Distributed traces you should find traces from both the test client and the server, remember its a little easier to filter to ingested traces.

[embedded image skipped: data URI was too large for staged Markdown]

Going deeper in one of the traces we see a service also got created,

[embedded image skipped: data URI was too large for staged Markdown]

Details about the running tests etc can be examined further in the traces views, the service view can act as a hub and should feature related metric information,

An example trace,

[embedded image skipped: data URI was too large for staged Markdown]

And the service view,

[embedded image skipped: data URI was too large for staged Markdown]

If service naming is carefully chosen automatic tags can also be added to give further aid when searching for and tracking events etc. but that will be later,,

[embedded image skipped: data URI was too large for staged Markdown]

This result can be checked in the service view,

[embedded image skipped: data URI was too large for staged Markdown]

That's the initial story! With the notes and repository anyone can start exploring the curious Open Telemetry space!

Blog tags

[linux](/taxonomy/term/7)

[ansible](/taxonomy/term/12)

[Dynatrace](/taxonomy/term/17)

[Opentelemetry](/taxonomy/term/20)

[Python](/taxonomy/term/21)

Submitted by auzieman
 on Tue, 12/20/2022 - 08:08

