---
title: "Cowabunga Kubernetes: Riding the Wave with Drupal on k3s!"
slug: "cowabunga-kubernetes-riding-the-wave-with-drupal-on-k3s"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/39"
source_id: "node-39"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

# Ride the Kubernetes Wave: Setting Up a Sandbox with k3s

**Cowabunga, fellow surfers!** Gather around, because I've got some rad news for you. The boss wants to ride the Kubernetes wave, and we're the ones to make it happen. But here’s the deal: before diving into the gnarly Kubernetes realm, we need a **safe sandbox**—a place to train, experiment, and master our skills without risking a major wipeout in the production environment.

Picture this: the boss comes up, shades on, and says, “Hey, dudes and dudettes, we’re going Kubernetes. We need a playground—like, yesterday!”

And that's where we step up—the Kubernetes gurus of the beach! We’ll set up a sandbox with [k3s](https://k3s.io), a lightweight and fast Kubernetes distribution, perfect for creating a local playground to test, spin up pods, and ride the containerized wave like pros. Let’s dive in!

## Why k3s?

**k3s** is built for those who want a lightweight Kubernetes experience. It’s perfect for sandbox environments and for testing out concepts before deploying them at full scale. By running k3s on a local or minimal setup, we get all the Kubernetes benefits with fewer resources and faster performance.

## Step-by-Step: Setting Up Your Kubernetes Sandbox

### 1. Hang Ten with a Minimal Ubuntu Install

Start by catching a gnarly wave with a minimal Ubuntu setup. Grab the [Ubuntu Server ISO](https://ubuntu.com/download/server) from the official website and carve it onto your machine. This keeps your environment lightweight and ready for some serious Kubernetes action.

### 2. Shred the k3s Installation

Now, it’s time to drop in k3s like a pro. Use the official installation script:

```
curl -sfL https://get.k3s.io | sh -
```

With this command, k3s will be up and running in no time, setting the stage for our Kubernetes sandbox.

### 3. Check the k3s Swell

Time to check if your k3s service is totally stoked:

```
systemctl status k3s
```

If everything’s active, you’re ready to catch some waves!

## Why Use Kubernetes with Drupal?

**Drupal** is a popular open-source CMS, perfect for testing Kubernetes setups because it requires multiple backend services to run smoothly. By containerizing Drupal, we can explore the scalability and flexibility of Kubernetes, deploying each service (like MySQL for the database) in its own pod, which Kubernetes can manage and scale as needed.

### Setting Up Drupal and MySQL in Kubernetes

#### Create a Kubernetes Namespace for Drupal

Let’s create a dedicated *namespace* for our Drupal application:

```
kubectl create namespace drupal
```

This namespace acts as a private beach for our app, where it can hang loose and catch some waves without interference from other applications.

#### Shape Your YAML Board

Now, let’s shape our YAML board. Here’s a full configuration file, setting up both Drupal and MySQL. Save it as `drupal-pod.yaml`:

```
apiVersion: v1
kind: Namespace
metadata:
  name: drupal
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mysql-pv-claim
  namespace: drupal
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
  name: mysql-primary
  namespace: drupal
spec:
  replicas: 1
  selector:
    matchLabels:
      app: mysql-primary
  template:
    metadata:
      labels:
        app: mysql-primary
    spec:
      containers:
        - name: mysql-primary
          image: mysql:5.7
          env:
            - name: MYSQL_ROOT_PASSWORD
              value: your-root-password
            - name: MYSQL_USER
              value: your-mysql-user
            - name: MYSQL_PASSWORD
              value: your-mysql-password
            - name: MYSQL_DATABASE
              value: your-database
          ports:
            - containerPort: 3306
              name: mysql
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: drupal
  namespace: drupal
spec:
  replicas: 1
  selector:
    matchLabels:
      app: drupal
  template:
    metadata:
      labels:
        app: drupal
  spec:
    containers:
      - name: drupal
        image: drupal:9
        env:
          - name: MYSQL_HOST
            value: mysql-primary
          - name: MYSQL_USER
            value: your-mysql-user
          - name: MYSQL_PASSWORD
            value: your-mysql-password
          - name: MYSQL_DATABASE
            value: your-database
        ports:
          - containerPort: 80
            name: drupal
---
apiVersion: v1
kind: Service
metadata:
  name: drupal
  namespace: drupal
spec:
  selector:
    app: drupal
  ports:
    - protocol: TCP
      port: 80
      targetPort: 80
  type: LoadBalancer
```

### Deploy the Configuration

Apply the YAML file to create your Drupal deployment and service:

```
kubectl apply -f drupal-pod.yaml
```

This sets up the pods and services to let your Drupal application ride the Kubernetes wave.

## Additional Resources to Level Up Your Kubernetes Game

Here are some resources to dive deeper into Kubernetes and enhance your knowledge:

### Online Courses:

- [**Kubernetes for Developers**](https://www.udemy.com/course/kubernetes-for-developers/) - A practical course to master Kubernetes as a developer.
- [**Learn Kubernetes by Doing**](https://www.udemy.com/course/learn-kubernetes-by-doing/) - Perfect for hands-on learners who want real-world Kubernetes skills.

### YouTube Tutorials:

- [**Kubernetes Crash Course**](https://www.youtube.com/watch?v=X48VuDVv0do) - A comprehensive beginner’s guide to Kubernetes.
- [**Setting Up k3s for Kubernetes**](https://www.youtube.com/watch?v=PH-2FfFD2PU) - An easy-to-follow guide on setting up k3s locally.

With your Kubernetes sandbox and these resources, you’re well on your way to mastering containerized environments. Let’s ride the Kubernetes wave to greatness!

Blog tags

[linux](/taxonomy/term/7)

[kubernetes](/taxonomy/term/49)

[ChatGPT](/taxonomy/term/23)

[drupal](/taxonomy/term/50)

[mysql](/taxonomy/term/14)

Submitted by auzieman
 on Fri, 06/02/2023 - 14:54

