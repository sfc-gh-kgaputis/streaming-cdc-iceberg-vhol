# Agent questions

The three questions to ask the **Cascade Plant Analyst**, verbatim, in this order. Keep this open
beside Snowsight; they are also inline in the README at Parts 4 and 5.

Ask them in the chat panel at **Snowsight → AI & ML → Agents → Cascade Plant Analyst**.

Order matters: the first two establish that the agent reads your pipeline correctly, and the third
needs both data sources at once.

---

## 1. Baseline — does it read the pipeline at all?

> What is first-pass yield by line right now?

**Expect:** three lines — WELD, PAINT, ASSEMBLY — each around 95–99% in steady state, and the agent
naming the 5-minute interval it used.

"Right now" means the most recent complete buckets, not the current minute: a 1–2 minute target lag
leaves the current minute empty.

## 2. Drill-down — can it get to the grain?

> Which defect is driving scrap on PAINT?

**Expect:** a ranked list of defect codes for PAINT, with `NONE` excluded.

`DEFECT_CODE = 'NONE'` means the scan passed, and it is the most common value in the table. An agent
that does not exclude it answers "the top defect is NONE".

## 3. Cause — can it get past what happened to why?

> Why did PAINT yield drop?

Ask this **during Part 5**, after the incident has propagated — roughly 5–7 minutes after you set the
simulator control mode to `INCIDENT`. The defect spike reaches `DEFECT_COUNTS_5MIN` sooner than that, but
yield needs enough of a 5-minute bucket to be affected before the drop is unmistakable. The producer keeps
running throughout; you never restart it.

**Expect:** the agent connects the **booth humidity rise** to the `PAINT_RUN` defects, and gets the
**order** right: humidity climbed first, defects followed. It should also note that WELD and ASSEMBLY
were unaffected.

Humidity comes from the telemetry feed and yield from the CDC feed, so neither source alone contains
this answer.

---

## Worth trying if you have time

Not scripted — watch how the agent copes with a question nobody built the pipeline for:

> Should I stop the PAINT line?

> Compare PAINT yield now against twenty minutes ago.

> Is anything wrong on WELD?

The honest answer to the last one is "no". If the agent manufactures a problem on a healthy line,
tighten its orchestration instructions.

---

Agent symptoms, causes and fixes are in [Troubleshooting](troubleshooting.md).
