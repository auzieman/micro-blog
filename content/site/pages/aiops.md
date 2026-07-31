---
page: aiops
title: AIOps That Can Show Its Work
body: Auzietek approaches AIOps as a governed operating loop: signal, resource context, bounded action, validation, receipt, and the next human decision.
tag: aiops
eyebrow: Human + AI operations
---

# AIOps That Can Show Its Work

AIOps is often described as anomaly detection, event correlation, and machine-assisted incident response. Those capabilities matter, but they are not enough on their own.

An operational system also needs to understand what a resource is, how it relates to other resources, which actions are safe, what evidence is required, and when a human must approve the next step.

BlackKnightController approaches AIOps as a governed operating loop rather than a chat window attached to monitoring.

## From signal to controlled action

The operating loop is:

```text
signal
  -> identify the affected resource
  -> gather current evidence
  -> relate dependencies and ownership
  -> select a bounded procedure
  -> request approval where required
  -> execute through reusable actions and fragments
  -> validate the result
  -> write a receipt
```

Each step should be inspectable. The system should be able to explain what it observed, why it selected a procedure, what it was authorized to do, and what changed.

## Resource-aware operations

A host is not only an IP address. A service is not only a port. In BlackKnightController, operational objects can carry relationships, ownership, capabilities, evidence, and available actions.

That model allows the system to reason about questions such as:

- Which edge service provides the route to this environment?
- Which virtual machines form this Swarm or K3s cluster?
- Which controller or operator owns the next decision?
- Which procedure is valid before an operating system exists?
- Which evidence proves that a deployment is healthy?

This is the foundation beneath useful AIOps. Without a durable resource model, an agent is forced to rediscover the environment during every run.

## Progressive trust

The safest path to useful automation is not immediate autonomy. It is progressive trust.

A workflow can begin in observation mode, then move through recommendation, approval-gated execution, and eventually bounded automatic execution for well-understood low-risk cases.

The trust level belongs to the action and context, not to a vague claim that the entire agent is trusted.

Examples:

- reading service state may be automatic;
- restarting a known non-production service may require a lightweight approval;
- changing DNS, certificates, or production routing requires explicit authorization;
- destructive rebuilds remain outside the procedure unless separately approved.

## Evidence before confidence

A green status should be supported by evidence, not optimism.

For a lab bring-up, useful evidence can include:

- BMC or IPMI power state;
- management reachability;
- firewall and route validation;
- hypervisor availability;
- cluster membership and convergence;
- service ports and health endpoints;
- application content checks;
- image-byte smoke tests;
- a final receipt naming exceptions and retries.

This distinction matters because infrastructure often enters a half-ready state. A virtual machine may be alive while its cluster membership is incomplete. A container runtime may be active while the orchestrator has not converged. AIOps should wait for the condition that matters instead of inferring success from a nearby green light.

## Deterministic procedures, bounded troubleshooting

Operational agents become expensive and unpredictable when a simple procedure turns into open-ended live troubleshooting.

BlackKnightController procedures should define:

- prerequisites;
- ordered states;
- success evidence;
- one or more explicitly allowed recovery actions;
- retry limits;
- stop conditions;
- the point at which human guidance is required.

When the system leaves the known procedure, it should say so early. “Outside procedure, manual approval required” is often a better result than twenty speculative shell commands.

## Human and agent collaboration

The human operator provides intent, risk judgment, and approval. The agent provides memory, consistency, evidence collection, and repeatable execution.

That division of labor is more useful than pretending either side should disappear.

A practical collaboration loop looks like this:

1. The operator requests an outcome.
2. The system resolves the relevant resources and procedure.
3. The system presents the bounded plan and required approval.
4. The agent executes only the authorized actions.
5. The system records evidence and exceptions.
6. The operator reviews the receipt and decides what happens next.

## Local-first and tool-friendly

BlackKnightController can work with existing infrastructure and operational tools rather than requiring every signal and action to move into one vendor platform.

Monitoring, Git, shell tooling, IPMI, hypervisors, container platforms, browser automation, and cloud APIs can remain specialized components. The controller provides the common operating fabric around them.

This local-first posture is important for labs, edge systems, regulated environments, and organizations that need to retain control of credentials, evidence, and execution paths.

## What the lab proves

The lab is not a decorative demo environment. It is a proving ground for the operating contract.

A single run can demonstrate:

- an approval entering through the private BKC channel;
- deterministic lab bring-up;
- edge and routing checks;
- ESXi and OpenStack host validation;
- Docker Swarm convergence;
- application and telemetry validation;
- a content refresh through a pinned deployment flow;
- browser choreography for visible proof;
- receipts passed between Astra, Codex, BKC, and the operator.

The point is not that every infrastructure problem has been solved. The point is that the system can move from intent to evidence without losing the boundary between observation, approval, execution, and proof.

## The result

Useful AIOps should reduce uncertainty, not hide it.

It should know when the world is ready, know when it is not, and leave enough evidence that another person or agent can continue without starting over.
