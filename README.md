# Workflow2

## Overview

Workflow2 is a DAG processor.  

## Why do I need a DAG processor?  Why can't I just write code?

You can!  But if your code is launching a series of remote big-data jobs (like a Spark or MapReduce job), stringing together those applications in a main method has drawbacks.  Running your code with a DAG processor provides a lot of features out of the box:

- **System visibility**: Humans can look at a UI to quickly see the status of the application.

- **Restartability**: If the application fails halfway through, you don't want to end in an inconsistent state or lose progress.

- **Reusability**: Sub-components of an application should be easy to share.

- **Analytics/history**: DAG frameworks capture historical timing info and failure histories.

- **Data framework integration**: Gather statistics from launched Hadoop applications and easily click through to their UIs.

- **Alerting**:  Get automatically alerted if the application fails or succeeds.

When a simple script turns into a multi-step application, it's probably time to start looking into a DAG processors to run it.

## So what is workflow2?

Workflow2 is the DAG processing framework LiveRamp uses to run all of its big data applications.

Workflow2 began (many, many years ago) as a simple in-memory DAG processor, and evolved into [blah]

## What distinguishes workflow2 from other DAG processors?


- DAGs -- both the structure and the actual operations -- are defined in Java

- Workflow2 was built with tight Hadoop integration (with an emphasis on MapReduce, and custom bindings for Cascading)

  - 

- Workflow2 is built to support very high numbers of concurrent applications and very high parallelism

  - LiveRamp runs > 100,000 DAG applications per day

  - Individual DAGs may have hundreds of steps, and run > 100 concurrently

  - At any given time, 500-1000 applications are generally running

  - tight hadoop (emphasis on mapreduce) integration
    - global views
    - global warnings
    - task exceptions


- overview

- help engineers write, maintain, debug big data applications
- give global views of infrastructure utilization


## Parts

A production deployment of workflow2 has a few moving parts:

- the __Workflow DB__ is a database which holds all system state

- The __Workflow UI__ is a web interface to allow users to view and modify system state

- The __Workflow Monitor__ is an asynchronous monitoring service which alerts users about poor-performing Hadoop applications and clients which have failed to heartbeat

- __Workflow Runner__ client processes submit and execute jobs

Each of these processes communicates exclusively through the Workflow DB instance:

![alt text](images/workflow_setup.png)

This is an important point: neither the UI or monitor need to be running for executors to submit and execute work.  The UI server is simply a UI.


## Concepts

_Actions_ run in a sequence defined by a DAG which is defined in user-code.  A very simple workflow looks like this:

```java
    Step step1 = new Step(new NoOpAction("step1"));

    Step step2 = new Step(new NoOpAction("step2"));

    Step step3 = new Step(new WaitAction("step3", 180_000), step1, step2);

    WorkflowRunners.dbRun(
        SimpleWorkflow.class.getName(),
        HadoopWorkflowOptions.test(),
        dbHadoopWorkflow -> Sets.newHashSet(step3)
    );
```

Our (very simple) Actions are defined in Java:

```java
public class NoOpAction extends Action {

  public NoOpAction(String checkpointToken) {
    super(checkpointToken);
  }

  @Override
  protected void execute() {
    // Deliberately do nothing.
  }
}

```

```java
public class WaitAction extends Action {

  private final long delay;
  
  public WaitAction(String checkpointToken, long delay) {
    super(checkpointToken);
    this.delay = delay;
  }

  @Override
  protected void execute() throws Exception {
    setStatusMessage("Sleeping for "+delay+" ms");
    Thread.sleep(delay);
  }
}
```

In this graph definition, Steps 1 and 2 can execute immediately.  Step 3 waits until both Steps 1 and 2 are complete, and then executes.  If we visualize this as a DAG, it looks like this:

![alt text](images/simple_dag.png)

When we call _WorkflowRunners.dbRun_, we tell the framework to do [asdf]

- Persist the workflow _structure_ to the database.  

- Begin executing steps, _in the local process_.  [To see how to execute code on remote workers, see BackgroundWorkflow]

When we initialize the workflow structure by invoking _WorkflowRunners.dbRun_, we create or re-use each of the following:

- We create __Step Attempts__ for every Step which will run

- We create a __Workflow Attempt__, which is conceptually "A set of steps which, if successful, will run an entire workflow"

- We either create or re-use a __Workflow Execution__.  If this is the first attempt at a workflow, create a new one.  If a previous attempt _failed_, we will re-use the previous execution.  More on this later.

- We either create or re-use an __Application__.  If this is the first time `com.liveramp.workflow2.workflow_examples.SimpleWorkflow` has ever run, we create a corresponding Application.

Because we told Workflow2 to execute against the database (dbRun), our state is entirely backed by the database, and we can track this state in the UI.  If we navigate to the Application's page, we can see all launched Executions:

![alt text](images/application.png)

Here we can see an old, complete Execution, and a new, running one.  We can click through to find more information about a specific Execution:

![alt text](images/execution.png)

We see there is a single, running, Attempt associated with this Execution.  Last, we can inspect the running Attempt: 

![alt text](images/workflow_attempt.png)

Here we see in detail the workflow we just launched.  We see it has 3 steps, and the first two have already executed; the last is still running.  

This is great, but what happens if a workflow fails halfway through?  Let's say our workflow failed on the second step:

![alt text](images/failed.png)

The next time we launch a workflow with the same name and scope (we'll talk about that later -- it default to null), the next _Attempt_ will pick up right where we left off:

![alt text](images/succeeded.png)

Notice that the first step was not re-executed -- it was skipped because it succeeded in a previous run.


TODO

multi step 

![alt text](images/multistep_collapsed.png)

![alt text](images/multistep_expanded.png)


scope 

- datastores, read create

- resources

 

## Getting started

how to spin up
- sql db
- simple manifest
- example workflow

## Non Hadoop workflows
  
hadoop vs non hadoop

hadoop integration
- runnablejob, etc
- queue
- properties

background
  
example code

notifications

## Hadoop integration

  - tight hadoop (emphasis on mapreduce) integration
    - global views
    - global warnings
    - task exceptions

## UI features

dashboards

manual completion

alerting

shutdown

died unclean

## Background Workflow


## Maven

## FAQ

#### Why is it called workflow2?

"Workflow" was already a bit overloaded.

#### Why doesn't the packaging line up with the maven artifacts?

Workflow2 is very widely used within LiveRamp, and repackaging would be a massive undertaking across our codebase; we prioritized OSSing the project over a 100% cleanup effort.  In a future major release we plan on aligning the packaging with the maven artifacts.

#### I like AirFlow better.

It's a free country, you can use AirFlow.

#### How can I contribute / get help / report bugs / learn more?

We love contributions!  The best way to get in touch is to create a ticket or a pull request.