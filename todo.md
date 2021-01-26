Open stuff
	- The call() syntax
	- magic turn-notebook-cell-into-job-thing
	- notebook interaction vs early exploding
	- 
	- robust history storing when the graph dies / is aborted during it's run?
	
	- general api polishing
	 
	- tests depending on partial MultiTempfile output 
	
	- interactive stuff
	

	- what happens when you run a partial graph somewhere else without access to
	  the original history? The FunctionInvariants clobber everything.
	  Ok, turn the off, what now?
	  The FileGeneratingJob.ouput_needed deletes the files.
	  
	  Is this the right way to go about this?

		Can we at least in interactive mode complain when we create a FileGeneratingJob
	    without history who's file already exists?
		
	  or should we simply share the damn status file? 
		
		
	- prebuild jobs
	 
	- filegenjobs temp output & rename (so that a crashing job doesn't get counted as 'done'),
	  though I believe that no longer to be an issue, since we don't update the history?
  
	- jobs overwriting the files of other jobs (without declaring so!)
	- multifilegeneratingjob,allow dict as output definition (see api polishing)

	- command runner job? kind of a replacement for mbf_externals.ExternalAlgorithm
	- 
	- - investigate inspect.signature to the lambda of: requirement, so we can throw right away.
	- 
	- write test case with our regular object structure
	- 
	- 
	- prevent stalling if a bunch of ALL_CORES jobs hog all the threads, no SINGLE_CORE jobs will be running.
