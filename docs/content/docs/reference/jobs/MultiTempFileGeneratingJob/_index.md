+++
title= "MultiTempFileGeneratingJob"
Weight= 12
+++

# ppg2.MultiTempFileGeneratingJob

A job that creates multiple files, which are deleted once all dependent jobs are done.

(See the [ephemeral jobs concept](../../concepts/ephemeral-jobs))

It is not removed if a dependant job fails.

For parameters see [MultiFileGeneratingJob](../multifilegeneratingjob).

-- 

If you want to preserve some files, while treating others as temporary,
you'll need to chain another job that copies the files-to-be-kept somewher safe. 

