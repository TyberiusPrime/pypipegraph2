Open stuff
	- FinalJobs (do we support these still)
	 
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
	- pruning
-

	- what happens when you run a partial graph somewhere else without access to
	  the orignal history? The FunctionInvariants clobber everything.
	  Ok, turn the off, what now?
	  The FileGeneratingJob.ouput_needed deletes the files.
	  
	  Is this the right way to go about this?

		Can we at least in interactive mode complain when we create a FileGeneratingJob
	    without history who's file already exists?
		
		
	- prebuild jbos
	- filegenjobs temp output & rename 
