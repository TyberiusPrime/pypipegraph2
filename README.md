# pypipegraph2


Add a short description here!


## Description

A longer description of your project goes here...


## Note

Differences to pypipegraph

	- FileGeneratingJobs now must take the target filename as first parameter
	  MultiFileGeneratingJob receive either their mapping, or their list of output files.
	- PlotJob now returns a tuple: (PlotJob, Optional(CalcJob), Optional(TableJob).
	  This removes all the unintuitive uglyness of 'which job will depends_on add the
	  dependency to'.
	- PlotJob skip_table / skip_caching are now create_table and cache_calc (defaulting to Ture)
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
