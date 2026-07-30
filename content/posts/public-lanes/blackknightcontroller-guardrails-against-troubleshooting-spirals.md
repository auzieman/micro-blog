---
title: Guardrails Against Troubleshooting Spirals
slug: guardrails-against-troubleshooting-spirals
summary: A candid BlackKnightController engineering note about protecting known-good pipeline fragments, finding the real failing boundary, and not repeating speculative fixes.
tags: [blackknightcontroller, guardrails, pipelines, pxe, debian, operations]
theme_variant: midnight
status: published
seo_title: BlackKnightController guardrails against troubleshooting spirals
seo_description: How BlackKnightController uses evidence, known-good fragments, candidate status, and focused validation to avoid repeated infrastructure troubleshooting spirals.
hero_image_url: /content-files/assets/bkc/bkc-pipeline-detail-edit.png
---

Good automation is not only about running faster.

Sometimes good automation means stopping the loop before it turns into churn.

During the Server1 Debian Trixie filming work, a common PXE installation path
expanded into repeated speculative changes. A run looked close enough to call
ready, but the final proof was missing: the server had to boot from its newly
written disk and pass first-boot validation without direct repair.

That gap mattered.

![BlackKnightController pipeline detail editor](/content-files/assets/bkc/bkc-pipeline-detail-edit.png)

## The required order after a regression

For a previously working pipeline, BKC should bias toward evidence before
implementation changes:

1. freeze the last known-good source and record commit/artifact hashes;
2. compare the failed run against that exact source, rendered artifacts, inputs,
   inventory, firmware state, and target hardware state;
3. identify the last positively observed boundary and the first missing one;
4. add read-only evidence at that boundary before changing behavior;
5. change one causal variable only;
6. run the smallest relevant automated test;
7. perform one real end-to-end attempt;
8. keep the change only when the intended end state is independently proven.

That sounds strict because it needs to be. A lab can be destructive and still be
disciplined.

## Evidence beats narrative

A bare-metal Debian run is not proven just because DHCP worked, the installer
downloaded, packages installed, or GRUB returned without an obvious error.

The real proof is closer to:

- expected disk identity and firmware mode before erasure;
- expected partition table and filesystems after installation;
- bootloader target appropriate to the active firmware mode;
- PXE disarmed before reboot;
- local-disk boot;
- expected host identity over SSH;
- deployment marker present;
- first-boot filesystem, network, SSH, and inventory validation complete.

Until then, the pipeline remains a candidate.

## Known-good fragments deserve protection

Once a script or template is end-to-end proven, it should get a fragment ID,
digest, contract test, proof run, and a warning label for future edits.

“Known good” does not mean the script ran once. It means the resulting machine
or service reached its validated end state.

That distinction protects the operator, the assistant, and the audience. During
filming or production-like work, the signal should be unambiguous:

```text
HOLD       -> candidate or diagnostic work remains
RECORD NOW -> exact deployed version completed its proof run
```

## The Server1 lesson

The eventual boundary was not mystical. Firmware, PERC, DHCP, iPXE,
kernel/initrd delivery, and preseed delivery had all been observed. The missing
boundary was local-disk first boot after Debian installation.

Later evidence showed Server1 was booting PXE with a Legacy override after
firmware changes made during the ESXi lane. The right response was not to invent
another installer recipe. The right response was to compare the firmware mode
against the last working run and correct that boundary.

That is exactly the kind of hard-won operational memory BKC should preserve.
