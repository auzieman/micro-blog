---
title: Docker Swarm for Small Teams Without the Drama
slug: docker-swarm-small-team-without-drama
summary: A cleaned-up Docker Swarm walkthrough based on the legacy Auzietek guide: minimal hosts, Docker CE, managers, workers, services, and practical checks.
tags: [linux, docker, swarm, containers, teaching]
theme_variant: linux-pro
status: published
seo_title: Docker Swarm guide for small Linux teams
seo_description: A practical Docker Swarm walkthrough covering host prep, Docker install, manager initialization, worker joins, services, and validation.
canonical_url: https://auzietek.com/node/38
---

Docker Swarm is still one of the clearest ways to teach container clustering.

Kubernetes won the mindshare war, but Swarm remains useful because the concepts
are visible: managers, workers, services, replicas, overlay networking, and
rolling updates. For a small lab or a teaching environment, that simplicity is a
feature.

The original Auzietek article had a playful “surfer” voice. The cleaned-up
version keeps the approachable spirit while making the steps easier to reuse.

## Start with minimal hosts

Use small Linux installs. Keep them boring.

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg
```

For Debian or Ubuntu, install Docker from the upstream repository or from your
distribution repository depending on how tightly you need version control.

```bash
curl -fsSL https://download.docker.com/linux/debian/gpg \
  | sudo gpg --dearmor -o /usr/share/keyrings/docker.gpg

echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker.gpg] \
https://download.docker.com/linux/debian trixie stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

## Initialize the manager

On the first manager:

```bash
docker swarm init --advertise-addr 10.20.0.51
```

Docker returns join commands for workers and additional managers. Save those in
your operational notes or, better, in a secure deployment fragment.

```bash
docker swarm join-token worker
docker swarm join-token manager
```

## Join workers

On each worker:

```bash
docker swarm join --token SWMTKN-... 10.20.0.51:2377
```

Then validate from a manager:

```bash
docker node ls
```

## Deploy a small service

Start with something boring:

```bash
docker service create \
  --name hello \
  --replicas 3 \
  --publish 8080:80 \
  nginx:alpine
```

Check placement:

```bash
docker service ls
docker service ps hello
curl -I http://10.20.0.51:8080/
```

## Compose becomes stack

The useful leap is from `docker compose` to `docker stack deploy`:

```bash
docker stack deploy -c docker-compose.yml demo
docker stack services demo
docker stack ps demo
```

That maps cleanly to BKC because a pipeline can render a Compose file, ship it,
deploy it as a stack, and validate the service URLs.

## Operational caveats

- Managers need stable addresses.
- Overlay networks require the Swarm control ports to be reachable.
- Persistent volumes need a plan.
- Published ports should be intentional.
- Secrets should not be baked into images or committed to Git.

The practical conclusion: Swarm is not the whole future of container
orchestration, but it remains an excellent “first cluster” and a reliable lab
target for teaching infrastructure automation.

Companion example:
[Docker Swarm for small teams](https://github.com/auzieman/auzietek-linux-lab-guides/tree/main/examples/docker-swarm-small-team).
