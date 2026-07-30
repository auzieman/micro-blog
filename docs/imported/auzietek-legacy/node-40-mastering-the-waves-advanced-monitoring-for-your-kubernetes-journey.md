---
title: "Mastering the Waves: Advanced Monitoring for Your Kubernetes Journey"
slug: "mastering-the-waves-advanced-monitoring-for-your-kubernetes-journey"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/40"
source_id: "node-40"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

Cowabunga, Kubernetes enthusiasts! Welcome to our guide where we're going to dive into setting up a gnarly monitoring system for our Kubernetes environment, using Prometheus, Grafana, and MariaDB.

**Introduction: Catching the Monitoring Wave** Our mission is to keep our Kubernetes beach safe and sound. Monitoring is our digital lifeguard, always on the lookout.

**Prometheus Pod:** This is our central data collector, the eagle eye that keeps tabs on the health and performance of our Kubernetes waves.

**Grafana Pod:** Here's where we get artsy. Grafana gives us the power to visualize the data collected by Prometheus. Think of it as our surfboard, letting us ride the data waves with style.

**MariaDB Pod:** Our treasure chest under the sea. MariaDB stores all our precious monitoring data, keeping our surf history safe.

We'll then guide you through setting up each component, with detailed YAML configurations for deploying Prometheus, Grafana, and MariaDB in a Kubernetes environment. We'll ensure these pods are properly networked within your Kubernetes cluster to communicate seamlessly.

Finally, we'll tie everything together, showcasing how these tools create an integrated monitoring system. This setup lets us monitor container health, resource usage, and more, ensuring our Kubernetes journey is smooth and rad!

Stay stoked, surfers! With this setup, you're ready to confidently ride the Kubernetes waves, knowing your monitoring setup has got your back. Keep surfing, and stay safe! Mind the storage defaults, could be crunchy for some hosts...

```
apiVersion: v1
kind: Namespace
metadata:
  name: monitoring
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: prometheus-pv-claim
  namespace: monitoring
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mariadb-pv-claim
  namespace: monitoring
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: prometheus
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: prometheus
  template:
    metadata:
      labels:
        app: prometheus
    spec:
      containers:
        - name: prometheus
          image: prom/prometheus:v2.26.0
          ports:
            - containerPort: 9090
          volumeMounts:
            - name: prometheus-storage
              mountPath: /prometheus
      volumes:
        - name: prometheus-storage
          persistentVolumeClaim:
            claimName: prometheus-pv-claim
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: grafana
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: grafana
  template:
    metadata:
      labels:
        app: grafana
    spec:
      containers:
        - name: grafana
          image: grafana/grafana:7.5.7
          env:
            - name: GF_DATABASE_TYPE
              value: mysql
            - name: GF_DATABASE_HOST
              value: mariadb.monitoring.svc.cluster.local:3306
            - name: GF_DATABASE_USER
              value: foo
            - name: GF_DATABASE_PASSWORD
              value: bar
            - name: GF_SECURITY_ADMIN_USER
              value: admin
            - name: GF_SECURITY_ADMIN_PASSWORD
              value: strongpassword
          ports:
            - containerPort: 3000
      volumes:
        - name: grafana-storage
          persistentVolumeClaim:
            claimName: grafana-pv-claim
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: mariadb
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mariadb
  template:
    metadata:
      labels:
        app: mariadb
    spec:
      containers:
        - name: mariadb
          image: mariadb:10.5
          env:
            - name: MYSQL_ROOT_PASSWORD
              value: your-root-password
          ports:
            - containerPort: 3306
      volumes:
        - name: mariadb-storage
          persistentVolumeClaim:
            claimName: mariadb-pv-claim
---
apiVersion: v1
kind: Service
metadata:
  name: prometheus
  namespace: monitoring
spec:
  selector:
    app: prometheus
  ports:
    - protocol: TCP
      port: 9090
      targetPort: 9090
  type: LoadBalancer
---
apiVersion: v1
kind: Service
metadata:
  name: grafana
  namespace: monitoring
spec:
  selector:
    app: grafana
  ports:
    - protocol: TCP
      port: 3000
      targetPort: 3000
  type: LoadBalancer
---
apiVersion: v1
kind: Service
metadata:
  name: mariadb
  namespace: monitoring
spec:
  selector:
    app: mariadb
  ports:
    - protocol: TCP
      port: 3306
      targetPort: 3306
  type: LoadBalancer
```

**Firing Up the Kubernetes Surf:**

Dudes and dudettes, it's time to catch some serious digital waves! Picture this: we’re on the beach, Kubernetes is our ocean, and each command we fire up is like carving a radical path through the gnarly surf. So grab your board and let's paddle out into the sea of data. We're going to tail logs like they're the perfect wave, and with each curl of load we generate on Drupal, we're riding high on the Kubernetes tide. Hang loose and watch as our services shred through the data swell!

```
#Assuming you named it like this get it running this way,
fcs-k3s1:/src/CatchDaMonitoringWave# kubectl apply -f CatchDaMonitoringWave.yml

namespace/monitoring created
persistentvolumeclaim/prometheus-pv-claim created
persistentvolumeclaim/mariadb-pv-claim created
deployment.apps/prometheus created
deployment.apps/grafana created
deployment.apps/mariadb created
service/prometheus created
service/grafana created
service/mariadb created

# Check the status of all services in the monitoring namespace
kubectl get services -n monitoring
NAME         TYPE           CLUSTER-IP      EXTERNAL-IP                 PORT(S)          AGE
grafana      LoadBalancer   10.43.223.203   192.168.1.50,192.168.1.70   3000:31102/TCP   2m22s
prometheus   LoadBalancer   10.43.239.10    192.168.1.50,192.168.1.70   9090:30723/TCP   2m22s
mariadb      LoadBalancer   10.43.195.55    192.168.1.50,192.168.1.70   3306:30189/TCP   2m22s

# Tail logs from the Prometheus pod
fcs-k3s1:/src/CatchDaMonitoringWave# PROMETHEUS_POD=$(kubectl get pods -n monitoring -l app=prometheus -o jsonpath="{.items[0].metadata.name}");kubectl logs -f $PROMETHEUS_POD -n monitoring
level=info ts=2024-01-29T19:38:06.109Z caller=main.go:380 msg="No time or size retention was set so using the default time retention" duration=15d
level=info ts=2024-01-29T19:38:06.110Z caller=main.go:418 msg="Starting Prometheus" version="(version=2.26.0, branch=HEAD, revision=3cafc58827d1ebd1a67749f88be4218f0bab3d8d)"
level=info ts=2024-01-29T19:38:06.110Z caller=main.go:423 build_context="(go=go1.16.2, user=root@a67cafebe6d0, date=20210331-11:56:23)"
level=info ts=2024-01-29T19:38:06.110Z caller=main.go:424 host_details="(Linux 6.6.13-200.fc39.x86_64 #1 SMP PREEMPT_DYNAMIC Sat Jan 20 18:03:28 UTC 2024 x86_64 prometheus-764565fc87-lrv7w (none))"
level=info ts=2024-01-29T19:38:06.110Z caller=main.go:425 fd_limits="(soft=1048576, hard=1048576)"
level=info ts=2024-01-29T19:38:06.110Z caller=main.go:426 vm_limits="(soft=unlimited, hard=unlimited)"
level=info ts=2024-01-29T19:38:06.114Z caller=web.go:540 component=web msg="Start listening for connections" address=0.0.0.0:9090
level=info ts=2024-01-29T19:38:06.116Z caller=main.go:795 msg="Starting TSDB ..."
level=info ts=2024-01-29T19:38:06.119Z caller=tls_config.go:191 component=web msg="TLS is disabled." http2=false
level=info ts=2024-01-29T19:38:06.124Z caller=head.go:696 component=tsdb msg="Replaying on-disk memory mappable chunks if any"
level=info ts=2024-01-29T19:38:06.124Z caller=head.go:710 component=tsdb msg="On-disk memory mappable chunks replay completed" duration=15.26µs
level=info ts=2024-01-29T19:38:06.124Z caller=head.go:716 component=tsdb msg="Replaying WAL, this may take a while"
level=info ts=2024-01-29T19:38:06.125Z caller=head.go:768 component=tsdb msg="WAL segment loaded" segment=0 maxSegment=0
level=info ts=2024-01-29T19:38:06.125Z caller=head.go:773 component=tsdb msg="WAL replay completed" checkpoint_replay_duration=71.575µs wal_replay_duration=640.108µs total_replay_duration=761.758µs
level=info ts=2024-01-29T19:38:06.128Z caller=main.go:815 fs_type=XFS_SUPER_MAGIC
level=info ts=2024-01-29T19:38:06.128Z caller=main.go:818 msg="TSDB started"
level=info ts=2024-01-29T19:38:06.128Z caller=main.go:944 msg="Loading configuration file" filename=/etc/prometheus/prometheus.yml
level=info ts=2024-01-29T19:38:06.130Z caller=main.go:975 msg="Completed loading of configuration file" filename=/etc/prometheus/prometheus.yml totalDuration=1.842042ms remote_storage=21.149µs web_handler=1.183µs query_engine=1.993µs scrape=940.28µs scrape_sd=208.827µs notify=41.492µs notify_sd=21.707µs rules=8.339µs
level=info ts=2024-01-29T19:38:06.130Z caller=main.go:767 msg="Server is ready to receive web requests."

# Tail logs from the Grafana pod
GRAFANA_POD=$(kubectl get pods -n monitoring -l app=grafana -o jsonpath="{.items[0].metadata.name}")
kubectl logs -f $GRAFANA_POD -n monitoring

# Generate some load on the Drupal service to see how our monitoring reacts
DRUPAL_SERVICE_IP=$(kubectl get svc -n drupal drupal -o jsonpath="{.spec.clusterIP}")
for i in {1..100}; do curl $DRUPAL_SERVICE_IP & done

# Watch the metrics in Prometheus and Grafana dashboards
echo "Navigate to Grafana dashboard to see the load impact"
```

***Bonus points,***

**Unleashing the `selenium_bot`:**

Now, let’s crank up the swell with our secret weapon – the selenium_bot! This little dude is like our custom wave machine, dialing up the surf so we can test our mettle against the raddest digital breakers. Installing Selenium IDE is like waxing our board, ensuring we're ready to grip those waves. We’ll record our session as if we’re mapping out the perfect surf line, then tweak our script to ride headless through the tubes. Once we deploy selenium_bot, it’s like unleashing a monster swell - our setup's gonna get a real taste of the surfer’s life. So let’s drop in, carve up these waves, and see if our digital beach can handle the selenium_bot storm!

1. **Install Selenium IDE**: Get your board ready by installing the Selenium IDE in your browser. It's like your digital surfboard wax.
2. **Record Your Session**: Hit the waves by recording a browsing session. Navigate through your site as a user would.
3. **Export and Tweak the Script**: Export this session as a script from Selenium IDE. You'll need to modify it for headless mode, which is perfect for running in a server environment.
4. **Deploy with `selenium_bot`**: Once your script is ready, deploy it with `selenium_bot`. This will create a realistic surfing scenario, testing how your setup handles real user interactions.
5. **Monitor and Analyze**: Keep a close eye on your monitoring tools. You'll see how your Kubernetes setup rides these simulated waves.

For detailed steps and more information, check out the [selenium_bot project page](https://dtlab.auzietek.com/auzieman/selenium_bot). Dive in, and let the good times roll!🌊🏄‍♂️🖥️🤙

Blog tags

[linux shell awk regex](/taxonomy/term/6)

[kubernetes](/taxonomy/term/49)

[grafana](/taxonomy/term/13)

[prometheus](/taxonomy/term/51)

[drupal](/taxonomy/term/50)

Submitted by auzieman
 on Sat, 12/09/2023 - 11:22

