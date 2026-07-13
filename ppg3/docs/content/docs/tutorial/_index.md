---
title: "Tutorial"
weight: 10
bookCollapseSection: false
---

# ppg3 tutorial

ppg3 is a **constructive-trace build system**. You describe your work as a
graph of *jobs* in a normal Python script; ppg3 figures out what has already
been built, runs only what is missing, and hands you a stable directory of
results.

The core promise is simple:

> Given the same inputs, a job's output is fetched from a content-addressed
> **store** instead of being recomputed. The first run builds everything; the
> second identical run is all cache hits.

This tutorial walks you from an empty directory to a working, cached pipeline,
explaining what happens **in your script**, **in the store**, and **in the
view** at each step.

## What you will build

A tiny two-job pipeline:

1. a `CommandJob` that shells out to write a greeting file, and
2. a `FileJob` — a Python callback — that reads the greeting and writes a
   summary.

Then you will run it twice (watching the cache kick in), flip a parameter
(watching only the affected job rebuild), inspect the on-disk store and view,
and finally meet the maintenance CLI.

## Steps

1. [Setup — installing and creating a graph]({{< relref "1-setup" >}})
2. [Your first pipeline — jobs, views, and `run()`]({{< relref "2-your-first-pipeline" >}})
3. [Inputs and caching — how ppg3 decides what to rebuild]({{< relref "3-inputs-and-caching" >}})
4. [DataJobs and the `io` object — passing Python values]({{< relref "4-datajobs-and-io" >}})
5. [Stores, views, and generations — what lands on disk]({{< relref "5-stores-views-generations" >}})
6. [The CLI and watch mode — maintaining a project]({{< relref "6-cli-and-watch" >}})

Each step builds on the last. If you just want the shortest path to a working
pipeline, read steps 1 and 2.
