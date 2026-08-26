# Agent questions

The three questions to ask the **Cascade Plant Analyst** agent, verbatim, in this order. They are also
inline in the lab README at Parts 4 and 5 — this file exists so you can keep it open beside Snowsight
instead of scrolling.

Ask them in the chat panel at **Snowsight → AI & ML → Agents → Cascade Plant Analyst**.

Order matters. The first two establish that the agent reads your pipeline correctly. The third is the
one that could not be answered without joining two data sources.

---

## 1. Baseline — does it read the pipeline at all?

> What is first-pass yield by line right now?

**Expect:** three lines — WELD, PAINT, ASSEMBLY — each around 95–99% in steady state, and the agent
naming the 5-minute interval it used.

**Why it is worth asking:** "right now" is ambiguous, and the agent has been instructed that it means
the most recent complete buckets rather than the current clock time. A pipeline with a deliberate
1–2 minute lag has no rows for the current minute, and an agent that does not know this reports
misleading emptiness instead of the latest real numbers.

## 2. Drill-down — can it get to the grain?

> Which defect is driving scrap on PAINT?

**Expect:** a ranked list of defect codes for PAINT, with `NONE` excluded.

**Why it is worth asking:** `DEFECT_CODE = 'NONE'` means the scan *passed*. It is the most common value
in the table and it is not a defect. An agent that includes it reports "the top defect is NONE",
which is both true and useless — the kind of answer that looks like a working agent and is not.

## 3. The payoff — can it find a cause?

> Why did PAINT yield drop?

Ask this **during Part 5**, after the incident has propagated — roughly 3–4 minutes after you set the
simulator control mode to `INCIDENT`. The producer keeps running throughout; you never restart it.

**Expect:** the agent connects the **booth humidity rise** to the `PAINT_RUN` defects, and gets the
**order** right: humidity climbed first, defects followed. It should also note that WELD and ASSEMBLY
were unaffected.

**Why this is the whole lab:** humidity comes from the Snowpipe Streaming telemetry feed. Defects and
yield come from the CDC feed. Nothing in either source alone contains the answer. It exists only
because `YIELD_BY_LINE_5MIN` joined them on line and 5-minute bucket in Part 3. An agent over the
CDC feed alone can tell you *what* happened and never *why*.

---

## If the answers look wrong

| What you see | What it means |
|---|---|
| An opaque `internal error`, code 391920 | The agent's Analyst tool has no warehouse. See Troubleshooting in the README. |
| Numbers that do not match the semantic view | Ask again in a minute — the pipeline lags 1–2 min by design. |
| "The top defect is NONE" | The agent is not excluding passed scans. Its orchestration instructions should say to. |
| Confident cause with the sequence backwards | It found the correlation but not the ordering. Ask it which came first. |
| A number that changed since your last question | Correct, and interesting. Inspectors overturn failed frames, which rewrites already-reported buckets. That is Part 5's recovery, not an error. |

## Worth trying if you have time

These are not scripted, and the agent may or may not handle them well — which is itself the useful
observation:

> Should I stop the PAINT line?

> Compare PAINT yield now against twenty minutes ago.

> Is anything wrong on WELD?

The last one is the interesting one. The honest answer is "no", and an agent that manufactures a
problem on a healthy line is telling you something about its instructions.
