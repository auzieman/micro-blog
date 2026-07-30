---
title: "Auz n Tek (gpt) present a valley talk version of getting to know Docker Swarm."
slug: "auz-n-tek-gpt-present-a-valley-talk-version-of-getting-to-know-docker-swarm"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/38"
source_id: "node-38"
captured_at: "2026-07-29"
candidate_lane: unreviewed
tags: [legacy, auzietek, needs-review]
assets:
  []
---

## Installation Notes for CentOS / Ubuntu

### Minimal Install, Dude!

To get started with Docker Swarm on CentOS or Ubuntu, we need to begin with a minimal install, ya know? So, fire up your favorite VM or bare metal server and do the following steps:

- Perform a minimal installation of your chosen OS. Keep it light, man!
- Once installed, update the system with the latest and greatest software packages using the command:

```
sudo yum update -y   # For CentOS
sudo apt update -y   # For Ubuntu
```

- Alright, now it's time to install Docker Community Edition (CE), the star of the show! Run the following commands to install Docker CE:

```
# For CentOS
sudo yum install -y yum-utils
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo yum install -y docker-ce docker-ce-cli containerd.io

# For Ubuntu
sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update -y
sudo apt install -y docker-ce docker-ce-cli containerd.io
```

- After Docker CE is installed, start the Docker service and enable it to start on system boot:

```
sudo systemctl start docker
sudo systemctl enable docker
```

- Great job, dude! Now, we're ready to initialize the Docker Swarm. Run the following command on your desired swarm master node:

```
sudo docker swarm init --advertise-addr <swarm1-ip>
```

1. Replace `<swarm1-ip>` with the IP address of your swarm master node. This will initialize the swarm and provide you with a command to join other nodes as managers or workers.
2. Copy the join command provided by the previous step and run it on your other nodes to join the Docker Swarm. Feel the unity, bro!

Alright, you're all set up with Docker Swarm using Docker Community Edition (CE) on CentOS or Ubuntu. Time to unleash the power of container orchestration!

## Spinning Up the Initial Workload

To get our Docker Swarm cluster up and running with an initial workload, we're gonna use the `drupal-mysql.yml` file you provided. It's gonna set up a MySQL database and two Drupal instances for some serious content management action. Here's what the `drupal-mysql.yml` file looks like:

```
version: '3.8'
services:
  mysql-primary:
    image: mysql:5.7
    deploy:
      replicas: 1
    environment:
      - MYSQL_ROOT_PASSWORD=your-root-password
      - MYSQL_USER=your-mysql-user
      - MYSQL_PASSWORD=your-mysql-password
      - MYSQL_DATABASE=your-database
    volumes:
      - /srv/mysql_drupal:/var/lib/mysqltmp
    networks:
      - drupal_network

  drupal1:
    image: drupal:9
    deploy:
      replicas: 1
    ports:
      - 8981:80
    environment:
      - MYSQL_HOST=mysql-primary
      - MYSQL_USER=your-mysql-user
      - MYSQL_PASSWORD=your-mysql-password
      - MYSQL_DATABASE=your-database
    depends_on:
      - mysql-primary
    volumes:
      - /srv/swarm_drupal:/opt/drupal
    networks:
      - drupal_network

  drupal2:
    image: drupal:9
    deploy:
      replicas: 1
    ports:
      - 8082:80
    environment:
      - MYSQL_HOST=mysql-primary
      - MYSQL_USER=your-mysql-user
      - MYSQL_PASSWORD=your-mysql-password
      - MYSQL_DATABASE=your-database
    depends_on:
      - mysql-primary
    volumes:
      - /srv/swarm_drupal:/opt/drupal
    networks:
      - drupal_network

networks:
  drupal_network:
```

To spin up this initial workload on your Docker Swarm cluster, follow these steps:

- Save the above `drupal-mysql.yml` file to your swarm manager node, dude!
- From the terminal, navigate to the directory where you saved the `drupal-mysql.yml` file.
- Deploy the services defined in the YAML file to your Docker Swarm cluster by running the following command:

```
sudo docker stack deploy -c drupal-mysql.yml drupal
```

- This command will create the MySQL service and two Drupal services in your Docker Swarm cluster.
- Give it a moment, bro! Docker Swarm will do its thing and start spinning up the services on your cluster. You can check the status and progress by running:

  - To view the tasks (containers) associated with a specific service, run:

```
sudo docker service ls
ID             NAME                         MODE         REPLICAS   IMAGE       PORTS
5qgvppexe5w0   drupal-stack_drupal1         replicated   1/1        drupal:9    *:8981->80/tcp
efqlji4gfwtc   drupal-stack_drupal2         replicated   1/1        drupal:9    *:8082->80/tcp
w084fy6x4g3k   drupal-stack_mysql-primary   replicated   1/1        mysql:5.7
```

- - This will show the tasks and their current state (e.g., running, pending, completed) for the `drupal` stack.

```
sudo docker stack ps drupal-stack
ID             NAME                               IMAGE       NODE      DESIRED STATE   CURRENT STATE          ERROR     PORTS
mo9o3yugkhm3   drupal-stack_drupal1.1             drupal:9    swarm2    Running         Running 3 hours ago
ve8nmlvgmnvw    \_ drupal-stack_drupal1.1         drupal:9              Shutdown        Shutdown 3 hours ago
7jg180x677hi   drupal-stack_drupal2.1             drupal:9    swarm1    Running         Running 3 hours ago
ll0769yjiymm    \_ drupal-stack_drupal2.1         drupal:9              Shutdown        Shutdown 3 hours ago
4x2d9e47dilh   drupal-stack_mysql-primary.1       mysql:5.7   swarm1    Running         Running 3 hours ago
v2votrrxby5k    \_ drupal-stack_mysql-primary.1   mysql:5.7             Shutdown        Shutdown 3 hours ago
```

Once the services are up and running, you can verify the connectivity between the Drupal instances and the MySQL container. To do this, we'll need to install some additional tools in the MySQL container. Connect to the MySQL container by running:

```
sudo docker exec -it <mysql-container-id> bash
```

- Replace `<mysql-container-id>` with the actual container ID of the MySQL container.
- Inside the MySQL container, update the package list and install the `iputils` package by running:

```
apt update
apt install -y iputils-ping
```

- This will ensure we have the necessary tools to perform network checks.
- Now, you can ping the Drupal instances from inside the MySQL container to verify connectivity. Run the following commands:

```
ping drupal1
ping drupal2
```

- If the pings are successful, it means the Drupal instances are reachable from the MySQL container, and everything is set up correctly.

Alright, dude! With these additional examples and verifications, you'll have a better understanding of the status of your Docker Swarm cluster and the connectivity between the services. Keep on riding the container wave and enjoy the power of Drupal in your scalable and resilient environment!

## Optional: Adding Nginx for Load Balancing and HTTPS

### Let's Get Swarmed with Some Load Balancing, Dude!

You've got your Docker Swarm up and running, and now you want to add some next-level load balancing and HTTPS support to your setup. No worries, bro! We'll hook you up with Nginx, the ultimate surfer dude of load balancers.

- First off, make sure you have an extra node in your Docker Swarm setup that will handle the load balancing. Let's call it "swarm-lb," the ultimate chill master.
- On the "swarm-lb" node, install Nginx using the following command:

```
sudo yum install nginx -y   # For CentOS
sudo apt install nginx -y   # For Ubuntu
```

- Once Nginx is installed, we need to configure it as a reverse proxy, load balancer, and HTTPS terminator for our Docker Swarm services. Edit the Nginx configuration file using your favorite text editor:

```
sudo nano /etc/nginx/nginx.conf
```

- Inside the `http` block of the Nginx configuration, add the following lines to define the upstream servers and enable load balancing

```
upstream docker-swarm {
    server <swarm1-ip>:8981;
    server <swarm2-ip>:8082;
}
```

- Replace `<swarm1-ip>` and `<swarm2-ip>` with the IP addresses of your Docker Swarm nodes running the Drupal services.
- Next, add a new `server` block in the Nginx configuration to define the virtual host for your load-balanced Drupal application and enable HTTPS. Here's a sample configuration:

```
server {
    listen 80;
    listen 443 ssl;
    server_name drupal.example.com;

    ssl_certificate /path/to/ssl_certificate.crt;
    ssl_certificate_key /path/to/ssl_certificate.key;

    location / {
        proxy_pass http://docker-swarm;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

- Replace `drupal.example.com` with your desired domain alias for accessing your Drupal application.
- Restart the Nginx service to apply the configuration changes:

```
sudo systemctl restart nginx
```

Alright, dude! You're all set up with Docker Swarm, Nginx load balancing, and HTTPS support for your Drupal application. Ride the wave of containerized awesomeness!

Submitted by auzieman
 on Thu, 06/01/2023 - 12:08

