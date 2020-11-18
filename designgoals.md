Designgoals:
	A pypipegraph-compatible library 
	that only recalculates jobs if their immediate input 
	truly changed (thus shielding from no-output-changing inputs),
	and that allows rerunning the job graph, and such an improved
	interactive (notebook) experience.
	
	It will also allow jobs that were absent temporarily
	to correctly discover whether their input changed.
	
	
API:
	The API will offer a pypipegraph compatibility layer,
	though I also want to replace some warts, so we'll see
	if there will be a new primary API.
	
	There will be one entry function
	ppg.new() with sensible logging defaults for our purposes.
	
	And one run function ppg.run() 
	though of course internally, there will be one ppg.global_pipegraph_object
	
	Calling a job will run the pipegraph as it is right now, and return a value, 
	which will allow jobs within the notebook that actually plot something.
	
	Creating a job will - as before, add it to the graph.
	
	FunctionInvariants should be able to auto-detect their name.
	
	ParamaterInvariants can be generated directly on existing jobs,
	auto-naming them.
	
	All FileGeneratingJob will be MultiFileGeneratingJob
	FGJ will just be a frontend.
	
	DataLoading will take two functions: load_data, unload_data, 
	and AttributeLoadingJob will just be a wrapper.
	
	Jobs will take a parameter add_function_invariant instead of 
	the auto_inject attribute hack we had.
	
	
Nomenclature:
	Upstream/downstream are jobs that need to be done before or after this job.
	(What the old pypipegraph called prerequisites and dependents).
	
	
History:
	Is stored per 'running script' - ie. per notebook filename.
	Will be placed, together with the logs, in folder .pypipegraph2
	
	The hashs will be dicts method -> hash allowing
	hash migration (when changing hash algorithm), 
	and keeping multiple 'hashes' (Think FunctionInvariants, 
	which are unchanged if either the byte code (python version specific!) 
	or the function source code is unchanged, 
	or FileInvariants, which only recalculate hashes when FileSize or MTime changed).
	
	
Jobs:
	Renaming a job between runs is 
	
	Inputs (derived from the direct upstream jobs):
		A list of (id, hash) tuples, being compared to the last run (the 'historical')
		An input is unchanged if there is exactly one input with the same hash
		allowing name changes.
		
		It is changed if the name is found, but the hash differs, triggering recalculation.
		
		It is missing, if it's not in the historical. A missing input triggers recalculation.
		
		Excess inputs compared to the historical will also trigger a recalculation.
		
		
	Outputs:
		A job generates (id, hash) output tuples.
		Creating actual output files is, in essence, a side effect.
		Missing output files might, or might not, trigger recalculation.
		A job may choose to return it's historical output instead (e.g. a FileGeneratingJob
		does not need to recalculate it's hash every time).
		
		The output-ids must be declared when the job is being created.
		A job might also have a api-name -> actual id mapping to allow 
		tying jobs to subparts of upstream jobs. (e.g. you may depend on the bam, but not on the BAI).
		
		
	Id: a job is identified by the ids it produces.
	    Renaming between runs is harmless, we match by hash.
		On id conflicts:
			if it's the same set of ids: replace job. Keep upstream/downstreams!
			If it's not:
				interactive: remove others, place this one, warn?
				non-interactive: hard fault, though optionally allow extension (thing QcCollectingJob).
				
				
		On redefinition of FunctionInvariants:
		FunctionInvariants should be 'instant-hashing'. Changing the contents *during* a run 
		is a GraphFailure. Changing it non interactively is a graph failure. 
		In interactive mode, warn and replace.
		
		Essentially the same goes for ParamaterInvariants.
		
		
Temp-jobs
		
		Temp-jobs will be handled by adding cleanup jobs after all their direct downstream jobs.
		Adding a job that depends on a temp-job in a job-generating-job will only be allowed 
		if the temp-job is also directly upstream of the job-generating-job, thus ensuring
		the cleanup-job won't have been run yet. But the job-generating-job must not be 
		just by it's existence trigger the temp-job generating.
		.
		
Job-generating-Jobs:
	Are allowed. Might trigger recalcs. If they ever get triggered a second time
	within one graph run, they explode (since otherwise you're halfway to an infinite
	loop).
	
	
Sidenodes: 
	All filenames will be Path.
	All job-ids will be str.
	Don't use tuples where a named tuple will do.
	
	
Logging:
	we use loguru, with these log levels 
		Error = graph died. 
		Warning = job died. Job was calculated twice because inputs changed during graph run.
		Success: job finished, runtime.
		Info (default) 
			- job recalc because of what
		Debug:
			trace all the things
	and some kind of sensible log rotation.
	
	What about interactive logging?
	
Job-Resources:
	We'll track job-resources a bit more fine grained than the old ppg did.
	Jobs may be
		- SingleCore (start as many as we have cores)
		- AllCores (one of these + one SingleCore allowed)
		- MemoryHog (one of these + any number of SingleCores/AllCores)
		- Exclusive (just this one job)
		- RateLimited (MaxInFlight, MaxRate...) (external jobs that are not actually limited on this system, but we shouldn't run 10.000 queries against
		   remote system at once).
		- 

Running:
	We'll keep the 'share everything read only' approach that ppg's forking used.
	That means keeping the dichotomy of jobs running in-process and out-of-process.
	
	No 'transfer jobs to other machines' is planned right now.
	
	
	running non-interactivily (ie. non notebook) should keep a simple CLI interface.
	Running within the notebook, We should see if we can offer something nicer.
	Or nothing at all, just like we had^^.
	
	
Performance: 
	We need to get the graph overhead so small, that we can essentially rerun (on no-op) interactively
	without delay. 
	We also need to multicore the hashing - though possibly, having FileInvariants be actually 
	(name, hash) producing jobs might alleviate this.
	This will probably also involve a faster way of writing (partial) history.
	
Questions:
	Do we need the FinalJob anymore?
	What about those qc collecting jobs, dirty hacks as they are.
	
	

