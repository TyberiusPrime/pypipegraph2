+++
title= "ExternalJob"
Weight= 10
+++

# ppg2.ExternalJob

A job that runs an external programm, logging the command, stdout, stderr, and return code.

## Parameters

* output_path - the folder where the output files will be stored.
* additional_created_files: A dictionary of {key: relative_path} that will be added to the job's output. 
  Downstream jobs can find the files by going job['key'].
* cmd_or_cmd_func - a list of strings that will be passed to subprocess.Popen, or a function that returns such a list.
  (shell=True is *not* being supported). The function will be called when the job is run, so after the dependencies have been 
processed.

## Optional Parameters
* allowed_return_codes: a list of return codes that will not lead to an exception / job failure. Default [0]
* call_before: A function to call just before the job is being run (possibly clean up old files)
* call_after: A function that's called after the external process has been run (clanup, post-processing, etc)
* job_cls: MultiFileGeneratingJob or MultiTempFileGeneratingJob
* resources: ppg.Resources (SingleCore | AllCores | Exclusive)
* cwd - the 'current working directory' of the external process. Defaults to output_path,
* start_new_session: Whether the job should be spawed in a 'new session' (see below)


## ExternalOutputPath
The command may contain entries like `ExternalOutputPath() / "somefile.txt"`. This will be replaced by the actual (absolute!) path
during the process run, but decouples your command definition from output_path.


## start_new_session
Passed to subprocess. To quote the subprocess documentation

```If start_new_session is true the setsid() system call will be made in the child process prior to the execution of the subprocess.```

That means the job will be decoupled from the terminal. Use it if the spawned process messes up the terminal otherwise.

(I suppose this could be the default. But linux process, process group, terminal control, session managment is really complicated,
and it's not the subprocess default - so we don't do it by default either. But when you need it, you need it).


