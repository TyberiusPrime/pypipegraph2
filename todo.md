Open stuff
	- DependencyInjectionJob (do we really need these still?)
	- FinalJobs (do we support these still)
	- PlotJobs
	- CachedDataLoadingJobs
	- CachedAttributeLoadingJobs
	- TempFilePlus jobs?? don't think we're using them *anywhere*
	- CombinedPlotJob?
	- notebook jobs?
	- MemMappedJobs (did we ever use these?
	 
	- 
	- The call() syntax
	- magic turn-notebook-cell-into-job-thing
	- notebook interaction vs early exploding
	- 
	- porting my million test cases
	- robust history storing when the graph dies / is aborted during it's run?
	
	- general api polishing
	 
	- tests depending on partial MultiTempfile output 
	
	- interactive stuff
	-
	- job renaming support (ie. rename a job, but refind it by it's inputs?)
 
	- 
	- data loading invalidation triggers
	- currently doesn't - running a dataloading job *always* triggers it's downstream 
	  to recalc
	-	attribute loading only triggers if the contents changed
	-   - deprecate dataloading, I suppose
	 
	- pretty and usable and appending runtime logging.
