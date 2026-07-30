# Public content proof autoprompt

Use this prompt before promoting Auzietek, BlackKnightController, Linux Users,
or Retro Users content.

## Rule

Do not publish beautiful text by itself when we have real evidence available.

Auzietek’s public posture is:

```text
validated -> published
no vaporware
```

That means public content should include proof when possible:

- screenshots from the running lab;
- diagrams generated from real inventory;
- terminal captures;
- pipeline snippets;
- GitHub links to runnable examples;
- known-good fragments;
- validation output;
- before/after comparisons.

## Editorial reminder

Write like a good classic computing magazine:

- strong article voice;
- practical explanation;
- enough screenshots to make the work tangible;
- excerpts instead of giant walls of code;
- links to GitHub for full artifacts;
- clear caveats and validation.

Keep the author voice human. Avoid letting the article sound like an AI
commentator reviewing Auzietek from outside the work. Prefer:

```text
Here is what we built.
Here is what still matters.
Here is the command, screenshot, repo, or caveat.
Here is how a learner can safely reach the next step.
```

over polished but distant narration.

## Modernization rule

It is fine to update older articles when the current world has moved on. For
example, a Puppet article may become an OpenVox-era configuration-management
guide, or an older ESXi backup script may become an ESXi 8 lab backup pattern.

But modernization must stay grounded:

- preserve useful original explanation and historical context;
- do not dismiss older techniques if they still teach or still work in labs;
- avoid judgmental phrasing about tools, vendors, or operators;
- clearly mark what is archival, what is current, and what is planned;
- for complex topics, prepare or link a runnable guide repo before making big
  claims;
- if the guide repo does not exist yet, frame the article as a roadmap or
  upcoming tutorial kit, not a completed walkthrough.

## Anti-spiral guardrail

If a content pass becomes abstract, stop and ask:

1. What did we actually build or validate?
2. What screenshot, command, graph, or repo proves it?
3. Is the article hiding the best evidence in a repo or chat transcript?
4. Should this be a blog article, a static page, or a tutorial kit?
5. Does the reader know where to go next?

If no evidence exists, label the content as concept/thinktank and do not present
it as a proven capability.

## Page-specific expectation

- Auzietek business pages need polished proof images, not stock art.
- BlackKnight pages need pipeline/resource screenshots or repo links.
- Linux Users articles need commands, expected output, and full repo links.
- Retro Users articles need project screenshots, build notes, or repo links.
- ThinkTank pages may be more conceptual, but should separate active prototypes
  from long-range ideas.
