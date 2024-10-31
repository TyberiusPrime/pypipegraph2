+++
title= "TempFileGeneratingJob"
Weight= 10
+++

# ppg2.TempFileGeneratingJob

A job that creates one file, which is deleted once all dependent jobs are done.

(See the [ephemeral jobs concept](../../concepts/ephemeral-jobs))

It is not removed if a dependant job fails.

For parameters see [FileGeneratingJob](../filegeneratingjob).

