+++
title= "ShellJob"
Weight= 15
+++

# ppg2.ShellJob

A job (helper, returns an [ExternalJob](../externaljob)) that runs a 
a shell script. The script get's written into the output folder and
made executable - you can use anything that you can run from a #! shebang.



## Parameters
* output_path - the folder where the output files will be stored.
* additional_created_files - A dictionary of {key: relative_path} that will be added to the job's output. 
  Downstream jobs can find the files by going job['key'].
* shell_script - The script, or a script producing function to call during the job's execution.

## Optional Parameters

All further **kwargs are passed to [ExternalJob](../externaljob).
