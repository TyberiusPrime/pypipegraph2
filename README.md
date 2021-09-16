# pypipegraph2


Finegrained tracking of what goes into generated artifacts,
and when do we actually need to recalculate them.

Also, trivial parallelization.


## Description

There's a bunch of 'pipeline' packages out there.
for scientific workflow managment.
Often with a lot of 'magic'.
SnakeMake is popular.

Pypipegraph2 is a bit different.

You build a directed, acyclic graph (DAG) of jobs that you want done.

A job is, for example,

 * a python function and the name of the file(s) it generates.
 * a python function
 * a name and a set of parameters
 * the name of an input file


Pypipegraph2 will hash all the things. Your input files,
your (intermediary) output files. Your parameters. Even
your python functions (source and bytecode).

When any of those change, or the number of inputs to a job changes,
we recalculate it. Only if it's output changed, we recalculate the immediate downstreams.
Do they return their old output (because your change was superficial)? Then we do not
recalculate their downstreams.

That's the big thing with respect to pypipegraph. When the input changed back then,
everything downstream was recalculated. 

You can use this in notebooks. You can use it in scripts. You can use it in
complicated scientific pipelines. It scales easily to a few 100.000 jobsjj

Here is the simples job:

```python
import pypipegraph2 as ppg2

ppg2.new()

def do_it(output_path):
	output_path.write_text("Hello world")
	
job = ppg2.FileGeneratingJob('hello.txt', do_it)
ppg2.run()
```

And you'll get two jobs: A FileGeneratingJob and a FunctionInvariant
to go with it.

If you change  `do_it`
and rerun your script, the output will change.

```
def do_it(output_path):
	output_path.write_text("Hello world, how are you today")
```

If you delete the output file, if you say `job.depends_on(ppg.FunctionInvariant(my_other_function))`,
if you remove such a dependency, 'hello.txt' will be rebuild.


For interactive work, you can redefine jobs and rerun the same graph again.

If jobs fail, those downstream of them / dependent on them will not be evalutated.
But everything outside of that part of the DAG will be.

Jobs will run in parallel, using both multi-threading 
(for jobs modifying the currently running program) and multi-process (for Filegenerating
jobs).

Jobs like AttributeLoading, and TempFileGenerating have cleanups that run 
when their immediate downstreams have been processed.

FunctionInvariants are *smart*. They compare bytecode if you're using the same 
python version, and fall back to source code if you have changed it.


# Jobs available


* `FileGeneratingJob(path, function)` -  generate one file
* `MultiFileGeneratingJob(paths | dict of paths, function)` -  generate multiple files
* `TempFileGeneratingJob(path, function)` -  generate one file, and remove it asap
* `MultiTempFileGeneratingJob(paths | dict of paths, function)` -  generate multiple files, and remove them asap
* `FunctionInvariant(name, function)` - track the source/bytecode of a function 
* FileInvariant(path) - track an input file
* `ParameterInvariant(name, (something, {'other': True}))` - track parameters
* `DataLoading(name, func)` - run this function in the current process
* `AttributeLoading(name, object, attribute_name, func)` - store the result of func on object
* `CachedDataLoading(path, calc_func, load_func)` - run func, cache the output of calc_func in a file, load_func(output) if required
* `AttributeLoading(path, object, attribute_name, func)` - store the result of func into a file, and load when necessary
* `JobGeneratingjob(name, func)` - generate more jobs (after the upstreams have run!)
* `PlotJob(output_path, calc_func, plot_func)` - generate some data, store it in a cache file, dump it a spreadsheet, generate a plot from the data, store it in output_path)
	


## Note

Differences to pypipegraph

	- graphs can now be run multiple times
	- calling a job will run the graph cut down to this job and it's prerequisites.
	  Some jobs - like PlotJobs will return something useful from the call.

	- FileGeneratingJob-callbacks now must take the target filename as first parameter
	  MultiFileGeneratingJob receive either their mapping, or their list of output files.
	  This is being checked at definitaion time.
	- MultiFileGeneratingJob may receive a dict of 'pretty name' -> filename'. 
	  Then you can depend_on(mfg['pretty name A']) to only invalidate when 'filenameA's content changes!
	- PlotJob now returns a tuple: (PlotJob, Optional(CalcJob), Optional(TableJob).
	  This removes all the unintuitive ugliness of 'which job will depends_on add the
	  dependency to'.
	- PlotJob skip_table / skip_caching are now create_table and cache_calc (defaulting to True)
	- CachedAttributeLoadingJob now returns a tuple (AttributeLoadingJob, FileGeneratingJob)
	- .ignore_code_changes has been replaced by constructor argument depend_on_function (inverted!)
	- ppg.run_pipegraph() / ppg.new_pipegraph() is now just ppg.run()/ppg.ew()
	- ppg.RunTimeError is now ppg.RunFailed

	- Removed the following due to 'no usage':
		- class MemMappedDataLoadingJob
		- class DependencyInjectionJob 
		- class TempFilePlusGeneratingJob 
		- class NotebookJob
		- class CombinedPlotJob
		- class FinalJob (was only used in the Bil)
		- PlotJob.add_fiddle
		- class JobList (depends_on handles all use cases without this special class / and it was unused)

		
	- a failed jobs exceptions are no longer available as job.exception,
      they can now be found in ppg.global_pipegraph.last_run[job_id].error
	  (last_run is also the result of ppg.run() if you set do_raise = False)

	- CycleError is now NotADag
	- ParameterInvariant no longer take an 'accept_as_unchanged' function. I suppose it would be trivial to implement using compare_hashes, but I couldn't find any current usage.
	- job + job (which returned a JobList) is no longer supported. Job.depends_on can be called with any number of jobs (this was already true in ppg1, but the + syntax was still around)
	- job.is_in_dependency_chain has been removed
	- the 'graph dumping' functionality has been removed for now
	- passing the wrong type of argument (such as a non callable to FunctionInvariant) raises TypeError instead of ValueError
	- FileGeneratingJob by default reject empty outputs (this can be changed with empty_ok=True) as a parameter. The default is inversed for MultiFileGeneratingJob.
	- In ppg1 if a file existed, a (new) FileGeneratingJob covering it was not run. PPG2 runs the FileGeneratingJob in order to record the right hash.
	- A failing job does no longer remove it's output. We know to rerun it because we didn't record it's new input hashes. This also means the rename_broken has been removed
	- TempFileGeneratingJob.do_cleanup_if_was_never_run is no more - I don't think it was ever used outside of testing
	- Defining multiple jobs creating the same output raises JobOutputConflict (more specific than ValueError)
	- The execution of 'useless' leaf jobs now usually happens at least once, due to them being invalidated by their FunctionInvariant
	- JobDiedException is now called just 'JobDied'
	- The various 'FileTimeInvariant/FileChecksumInvariant/RobustFileChecksumInvariant' forwarders have been removed. Use FileInvariant.
	- MultiFileInvariant is gone. Adding/removing FileInvariants now triggers by itself, no need to stuff multiple into a MultiFileInvariant
	- Pruning + running will no longer set ._pruned=pruning_job_id on downstream jobs, but .pruned_reason=pruning_job_id. Otherwise you could not unprune() and run again.
	- ppg.util.global_pipegraph is now ppg.global_pipegraph
	- Redefining a job in an incompatible way now raises JobRedefinitionError (instead of JobContractError)
	- Calling the same PlotJob once with cache_calc/create_table = True and once with False no longer triggers an exception, even in strict (RunMode.CONSOLE) mode. The jobs do stick around though.
	- interactive console mode
		- restart/reboot is now 'again/stop_and_again' to make it clearer
		- better progression, nice output 
		- some barely used commands were removed for now (runtimes (see log/runtimes.tsv), next, stay, errors, spy, spy-flame
	
	- new job kind: SharedMultiFileGeneratingJob
      This job's output folder is keyed by a hash of it's inputs, and can be easily shared between multiple ppgs from multiple places (replaces mbf_externals.PreBuildJob, conceptually)
