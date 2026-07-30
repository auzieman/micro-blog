---
title: "Docker / Server Naming Patterns and Dynatrace Auto-Tagging, Surfer Style"
slug: "docker-server-naming-patterns-and-dynatrace-auto-tagging-surfer-style"
summary: "Legacy Auzietek capture staged for cleanup, tagging, and lane assignment."
status: draft
source_url: "https://auzietek.com/node/31"
source_id: "node-31"
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
---

Yo, brah! So, we're diving into the rad world of Docker naming patterns and Dynatrace auto-tagging, dude. These patterns are all about organizing and managing your containers like a boss, making it a breeze to find what you need in Dynatrace, the ultimate wave-rider's tool.

Alright, let's talk naming patterns, man. We're all about using hyphens and underscores to separate the parts of the string, ya know? Picture this:

```
app-123_myservice.lab docker build -t app-123_myservice.lab
```

or..

```
docker run -d --name app-123_myservice.lab app-123_myservice.lab
```

We kick things off with a sick prefix like "app-123," followed by the service name, which in our case is "myservice." And to top it off, we add the environment tag, like "lab." Hyphens for the prefix and environment, and an underscore to separate the service name, dude.

Now, let's ride the auto-tagging wave in Dynatrace, bro:

In this naming pattern:

- app-123 is the prefix
- myservice is the service name
- lab is the environment

We kick things off with a sick prefix like "app-123," followed by the service name, which in our case is "myservice." And to top it off, we add the environment tag, like "lab." Hyphens for the prefix and environment, and an underscore to separate the service name, dude.

Now, let's ride the auto-tagging wave in Dynatrace, bro:

First, get yourself into your Dynatrace account, man.

Hit up the navigation menu and cruise on over to "Settings," then "Tags," and finally "Automatically applied tags."

Boom! You're gonna click that "Create new auto-tagging rule" button. Get ready to ride!

[embedded image skipped: data URI was too large for staged Markdown]

Here comes the fun part, dude. Under "Rule type," choose "Services." That'll open up the "Optional Tag value" section, where you can access the "Docker image name" or "Docker container name," depending on what you're after. We're rollin' with "ContainerName," bro.

[embedded image skipped: data URI was too large for staged Markdown]

Now, let's add some conditions, man. Hit up "Add Condition" and select the property you wanna check. In our case, we're goin' with "Docker container Name." Choose "contains" from the dropdown list and drop in an underscore "_" in the input field. Keep it groovy!

[embedded image skipped: data URI was too large for staged Markdown]

Time to tag it up, bro. Head on over to the "Tag" section and click on "Use placeholders." This is where the magic happens.

In the "Tag name" field, enter "Docker-ServiceName." That's gonna be our rad tag name, dude.

Now, in the "Tag value" field, it's placeholder time. Drop in this beauty: "{DockerContainerGroupInstance:ContainerName/^.*_([^.]++)}." That's gonna extract the gnarly "myservice" part from the image or container name, man. Super cool, right?

[embedded image skipped: data URI was too large for staged Markdown]

Hit "Save," and you're good to go, dude!

With this awesome auto-tagging rule, Dynatrace will slap a tag on your service with the key "Docker-ServiceName" and the extracted value, which is your killer service name.

But wait, there's more! Duplicate this pattern for some additional tags, man:

- Tag name: docker-appID. Apply the rule "{DockerContainerGroupInstance:ContainerName/^([^_]++)}" to Services on Process groups where Docker container name exists.
- Tag name: Docker-Environment. Apply the rule "{DockerContainerGroupInstance:ContainerName/^._..([^.]++)}" to Services on Process groups where Docker container name exists.

Now, with these epic tags in place, you're ready to ride the waves, dude!

Validate it on a matching container service like you see here, bro:

[embedded image skipped: data URI was too large for staged Markdown]

And check this out! You can perform searches in Distributed Traces like this sweet example:

[embedded image skipped: data URI was too large for staged Markdown]

As you keep building more services like a true surfer pro, these tags will become your ultimate weapon for finding and analyzing traces using Dynatrace's Davis AI. And if things get gnarly with complex, layered services, you can even add additional tags like "function" to slice and dice 'em further.

Stay stoked and keep riding the container management waves, bro!

Blog tags

[Dynatrace](/taxonomy/term/17)

[docker](/taxonomy/term/11)

Submitted by auzieman
 on Tue, 04/11/2023 - 17:38

