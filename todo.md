Open stuff
	- magic turn-notebook-cell-into-job-thing
 
	- notebook interaction vs early exploding (mostly testing)
	- 
	- robust history storing when the graph dies / is aborted during it's run?
	   - I suppose we could handle this with a log that we reapply later on?
	
	- jobs overwriting the files of other jobs (without declaring so!)
	  a hard problem, with little payoff?
	 
	- command runner job? Kind of a replacement for mbf_externals.ExternalAlgorithm
	 
	- write test case with our regular object structure
	 
	-ppg1 compatibility layer
	
	- tests for the interactive (console) parts.

	- a solution for the quadratic parts in modify_dag
	
	- logs/errors are not stored in per-run folders -are not being cleared.
	
	- test case for cache_folder

	 
	 - would it make sense to put the cleanups into the outputs, so the outputs 
	   can say 'and remove this file' - that way we could do a TempFilePlusGeneratingJob again?
	 
	- should we switch the history to json? or sqlite? if the later, we could update *during* 
	  the runs...
	  
	- # bump version above ppg1

	
	- should be not create a ppg at startup - like ppg1 did, and for interactive use?

	
	- test what happens if you have two fileinvariants as input and you swap them. No recalc, right?
	  what if they were foreground/background? this could be a problem
