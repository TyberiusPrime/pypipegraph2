Open stuff
	- magic turn-notebook-cell-into-job-thing

	- can we not reavaluate function invariants for jobs using identical functions
	  (but rebound variables, I suppose)?
	- can we somewhow deduplicate the above when dumping the history (though I suppose gzip does a decent job?).
	  (yes it does... for example 100k jobs have 138mb of history that get's compressed down to 7.5mb.. but still, 
	  would be better to not have this is in memory at all.)
 
	- notebook interaction vs early exploding (mostly testing)
	- 
	- robust history storing when the graph dies / is aborted during it's run?
	   - I suppose we could handle this with a log that we reapply later on?
	
	- jobs overwriting the files of other jobs (without declaring so!)
	  a hard problem, with little payoff?
	 
	- command runner job? Kind of a replacement for mbf_externals.ExternalAlgorithm
	 
	- write test case with our regular object structure -> nah, the mbf_* tests will do that for us.
	 
	- tests for the interactive (console) parts.

	- a solution for the quadratic parts in modify_dag - details?
	
	- test case for cache_folder
	 
	 - would it make sense to put the cleanups into the outputs, so the outputs 
	   can say 'and remove this file' - that way we could do a TempFilePlusGeneratingJob again?
	 
	- should we switch the history to json? or sqlite? if the later, we could update *during* 
	  the runs...
	  
	- output aware data loading jobs that return a useful hash.

	
	- test what happens if you have two fileinvariants as input and you swap them. No recalc, right?
	  what if they were foreground/background? this could be a problem.
	  This is a problem.
	  If you add a ParameterInvariant, you will retrigger on renaming the inputs, even if they stay the same.
	  What about giving the fileinvariants an additional 'name', and only searching for renamed matches in that?
	  No that won't fly, the same fileinvariant may be used by multiple jbos
	  Good ideas needed.
	- How about a FileOrderInvariant(FileInvariant, FileInvariant, FileInvariant,...)? it's hash would be the 
	  concatenation of the input hashes (or a hash of such a concatenation), that could be robust against 
	  file renaming?
	 
	- speed. We are much slower than pypipegraph1 on the (compatibility) test suite, which is a subset
	  of the ppg1 test suite. ppg1 run's it's testsuite in 26 seconds. ppg2 runs the compatibility test suite
	  (fewer test!) in 81 seconds.
	  
	  Profiling suggests two contenders: 20% in threading.lock, and 50% somewhere downstream from logger.emit / rich.console.print
	  disabling rich.console as loguru sink doesn't help much though.
	  Actually, handle_job_failed apparently is 'expensive' in terms of console.print (40% of total runtime?)
	  doing straight file write instead of console.print saves 'only' about 5% though.
	  (this might have been based on profiling just test_other.py though)
	  
	  profiling test_invariants_and_dependencies...
	  says 50%  in logger.debug, handle_success, runner:462
	  that would be 'logging runtime. Especially for jobs <= 1s, this is a factor of 2x on test_invariants_and_dependencies...
 	  (but ppg1 compatibility is at 128s with the line removed. And at 128 with it. So important for some, but not all tests)
	  bumping the log level has dramatic effects on this file.
	- error: 7.61
	  Info: 11.53.
	  Debug: 21.53
	
	ppg1 compatibility tests: 128s
	with log_level = error:  87 seconds
	what does the profiling with log_level == error say?
	-lot's of waiting (25%)
	- job_trace function call - 35%
	disabling job_trace log, by early return 
	55.5s
	without any logging:
	33s. 75% of my runtime, right there...
	-> profiling without the log...
	- andit's 75% waiting for the lock for the que.
	(no logging, test_invariants_and_dependencies ais down to 6s.
	
	ok, lookin at the remainder with pyspy, without suprocesse:
	top is FG.run -  that's basically expected (and mostly the plotjobs being tested)
	- a suprising 0.25s are being spend in 'islink'
	- onether 0.2s in namedtuple
 
    ok, py-spy informed me that we spend much time importing plotnine - importing
	it in conftest saves 10s on the ppg1_compatibility tests, which brings us 
	at par with ppg1 (23s ppg2 vs 26s ppg1)
	total test suite in 75s... not too bad.
	I have at least one test that's running interactively though?
	
	but utterly disabling the logging is not an option :)
	
	
	ok, disabling job_trace_log if not needed, redirecting all logging trough my own custom functions,
	we're at 112 s for the whole testsuite, 45s for the compatibility tests.
	Not sure why the later does anything, but it does seem to help?
	early returning from that we're back at 33.51s.
	
	Still 10 seconds more than without any logging.
	just warning & error -> 41.7s
	just info & debug -> 29.79 (huh)
	 error & info & debug-> 44s
	 warning & info & debug ->  44s
	 
	 those 10s are rich's doing btw.
	 full logging, but no rich: 36s.
	 seems rich is mostly those 10s one time overhead...
	 actually, that's the 'rich-to file', not the 'rich to terminal'...
	 
	 no logging, no rich: 24.43
	 no rich, but logging:  27.77s
	 
	 total without rich:  77s
	 
	

	
	 
	
	
	I wonder if all the logging issues is actually pytest overhead?
	 
	  
	   
	- should I convert the history to json?
	- 
	
	- - test case for running
	- does adding an *output* file correctly triggere a rebuild? how about removing one?


   - stop when having jobGeneratingJobs not working


- recheck everything above
   - sort interactive jobs by 'status' (running, waiting)
   - much better default log output... tell me what jobs spawned & finished

- check the runtime log, I think it's sometimes missing newlines?


Handlet he timeouts:


Found 237 mutants to test
ok       Unmutated baseline in 4s build + 0s test
 INFO Auto-set test timeout to 20s
MISSED   src/engine.rs:107:13: delete match arm JobState::Always(JobStateAlways::FinishedUpstreamFailure) in JobState::is_upstream_failure in 0s build + 0s test
MISSED   src/engine.rs:109:13: delete match arm JobState::Ephemeral(JobStateEphemeral::FinishedUpstreamFailure) in JobState::is_upstream_failure in 0s build + 0s test
MISSED   src/engine.rs:129:9: replace <impl JobQueries for JobStateAlways>::is_failed -> bool with true in 0s build + 0s test
MISSED   src/engine.rs:188:9: replace <impl JobQueries for JobStateEphemeral>::is_failed -> bool with true in 0s build + 0s test
MISSED   src/engine.rs:211:9: replace NodeInfo::clone_job_id -> String with String::new() in 0s build + 0s test
MISSED   src/engine.rs:211:9: replace NodeInfo::clone_job_id -> String with "xyzzy".into() in 0s build + 0s test
MISSED   src/engine.rs:273:9: replace Signal::job_id -> &'a str with "" in 0s build + 0s test
MISSED   src/engine.rs:273:9: replace Signal::job_id -> &'a str with "xyzzy" in 0s build + 0s test
MISSED   src/engine.rs:428:9: replace PPGEvaluator<T>::contains_node -> bool with false in 0s build + 0s test
TIMEOUT  src/engine.rs:467:9: replace PPGEvaluator<T>::is_finished -> bool with false in 0s build + 20s test
MISSED   src/engine.rs:484:9: replace PPGEvaluator<T>::debug_is_finished -> bool with true in 0s build + 0s test
MISSED   src/engine.rs:484:9: replace PPGEvaluator<T>::debug_is_finished -> bool with false in 0s build + 0s test
MISSED   src/engine.rs:485:16: delete ! in PPGEvaluator<T>::debug_is_finished in 0s build + 0s test
MISSED   src/engine.rs:494:9: replace PPGEvaluator<T>::debug_ -> String with String::new() in 0s build + 0s test
MISSED   src/engine.rs:494:9: replace PPGEvaluator<T>::debug_ -> String with "xyzzy".into() in 0s build + 0s test
MISSED   src/engine.rs:499:9: replace PPGEvaluator<T>::reconsider_all_jobs -> Result<(), PPGEvaluatorError> with Ok(()) in 0s build + 0s test
MISSED   src/engine.rs:501:16: delete ! in PPGEvaluator<T>::reconsider_all_jobs in 0s build + 0s test
MISSED   src/engine.rs:510:9: replace PPGEvaluator<T>::debug -> String with String::new() in 0s build + 0s test
MISSED   src/engine.rs:510:9: replace PPGEvaluator<T>::debug -> String with "xyzzy".into() in 0s build + 0s test
MISSED   src/engine.rs:565:9: replace PPGEvaluator<T>::verify_order_was_topological -> bool with true in 0s build + 0s test
MISSED   src/engine.rs:565:9: replace PPGEvaluator<T>::verify_order_was_topological -> bool with false in 0s build + 0s test
MISSED   src/engine.rs:568:65: replace == with != in PPGEvaluator<T>::verify_order_was_topological in 0s build + 0s test
MISSED   src/engine.rs:569:67: replace == with != in PPGEvaluator<T>::verify_order_was_topological in 0s build + 0s test
MISSED   src/engine.rs:575:26: replace > with == in PPGEvaluator<T>::verify_order_was_topological in 0s build + 0s test
MISSED   src/engine.rs:575:26: replace > with < in PPGEvaluator<T>::verify_order_was_topological in 0s build + 0s test
MISSED   src/engine.rs:575:26: replace > with >= in PPGEvaluator<T>::verify_order_was_topological in 0s build + 0s test
MISSED   src/engine.rs:589:9: replace PPGEvaluator<T>::next_job_ready_to_run -> Option<String> with None in 0s build + 0s test
MISSED   src/engine.rs:589:9: replace PPGEvaluator<T>::next_job_ready_to_run -> Option<String> with Some(String::new()) in 0s build + 0s test
MISSED   src/engine.rs:589:9: replace PPGEvaluator<T>::next_job_ready_to_run -> Option<String> with Some("xyzzy".into()) in 0s build + 0s test
MISSED   src/engine.rs:597:17: delete match arm JobState::Always(JobStateAlways::Running) | JobState::Output(JobStateOutput::Running) | JobState::Ephemeral(JobStateEphemeral::Running(_)) in PPGEvaluator<T>::query_jobs_running in 0s build + 0s test
MISSED   src/engine.rs:698:50: replace == with != in PPGEvaluator<T>::new_history in 0s build + 0s test
MISSED   src/engine.rs:769:65: replace || with && in PPGEvaluator<T>::new_history in 0s build + 0s test
MISSED   src/engine.rs:823:29: delete match arm JobState::Output(JobStateOutput::FinishedSkipped) | JobState::Ephemeral(JobStateEphemeral::FinishedSkipped) in PPGEvaluator<T>::new_history in 0s build + 0s test
MISSED   src/engine.rs:834:49: delete match arm JobState::Ephemeral(_) in PPGEvaluator<T>::new_history in 0s build + 0s test
TIMEOUT  src/engine.rs:891:9: replace PPGEvaluator<T>::event_startup -> Result<(), PPGEvaluatorError> with Ok(()) in 0s build + 20s test
MISSED   src/engine.rs:925:29: delete match arm JobState::Ephemeral(_) in PPGEvaluator<T>::prune_leaf_ephemerals in 0s build + 0s test
TIMEOUT  src/engine.rs:993:9: replace PPGEvaluator<T>::event_job_finished_success -> Result<(), PPGEvaluatorError> with Ok(()) in 0s build + 20s test
TIMEOUT  src/engine.rs:1165:9: replace PPGEvaluator<T>::process_signals -> Result<(), PPGEvaluatorError> with Ok(()) in 0s build + 20s test
TIMEOUT  src/engine.rs:1173:9: replace PPGEvaluator<T>::inner_process_signals -> Result<(), PPGEvaluatorError> with Ok(()) in 0s build + 20s test
MISSED   src/engine.rs:1173:18: replace > with == in PPGEvaluator<T>::inner_process_signals in 0s build + 0s test
MISSED   src/engine.rs:1173:18: replace > with >= in PPGEvaluator<T>::inner_process_signals in 0s build + 0s test
MISSED   src/engine.rs:1295:29: delete match arm JobStateEphemeral::NotReady(ValidationStatus::Invalidated) in PPGEvaluator<T>::inner_process_signals in 0s build + 0s test
MISSED   src/engine.rs:1477:25: delete match arm JobState::Ephemeral(JobStateEphemeral::FinishedSkipped) in PPGEvaluator<T>::inner_process_signals in 0s build + 0s test
TIMEOUT  src/engine.rs:1588:12: delete ! in PPGEvaluator<T>::inner_process_signals in 0s build + 20s test
MISSED   src/engine.rs:1597:40: replace + with * in PPGEvaluator<T>::inner_process_signals in 0s build + 0s test
MISSED   src/engine.rs:1656:17: delete match arm JobState::Always(_) in PPGEvaluator<T>::all_downstreams_validated_or_upstream_failed in 0s build + 0s test
TIMEOUT  src/engine.rs:1685:9: replace PPGEvaluator<T>::all_upstreams_done -> bool with false in 0s build + 20s test
MISSED   src/engine.rs:1695:9: replace PPGEvaluator<T>::has_upstreams -> bool with true in 0s build + 0s test
MISSED   src/engine.rs:1714:9: replace PPGEvaluator<T>::try_finding_renamed_multi_output_job -> Option<String> with None in 0s build + 0s test
MISSED   src/engine.rs:1714:9: replace PPGEvaluator<T>::try_finding_renamed_multi_output_job -> Option<String> with Some(String::new()) in 0s build + 0s test
MISSED   src/engine.rs:1714:9: replace PPGEvaluator<T>::try_finding_renamed_multi_output_job -> Option<String> with Some("xyzzy".into()) in 0s build + 0s test
MISSED   src/engine.rs:1738:28: replace > with == in PPGEvaluator<T>::try_finding_renamed_multi_output_job in 0s build + 0s test
MISSED   src/engine.rs:1738:28: replace > with < in PPGEvaluator<T>::try_finding_renamed_multi_output_job in 0s build + 0s test
MISSED   src/engine.rs:1738:28: replace > with >= in PPGEvaluator<T>::try_finding_renamed_multi_output_job in 0s build + 0s test
MISSED   src/engine.rs:1961:26: replace += with -= in PPGEvaluator<T>::update_validation_status in 0s build + 0s test
MISSED   src/engine.rs:1996:9: replace PPGEvaluator<T>::propagate_job_required with () in 0s build + 0s test
TIMEOUT  src/engine.rs:2025:9: replace PPGEvaluator<T>::signal_consider_job -> Result<(), PPGEvaluatorError> with Ok(()) in 0s build + 20s test
MISSED   src/engine.rs:2140:29: replace && with || in PPGEvaluator<T>::signal_consider_job in 0s build + 0s test
MISSED   src/engine.rs:2303:21: delete match arm JobState::Output(JobStateOutput::NotReady(ValidationStatus::Invalidated)) in PPGEvaluator<T>::downstream_requirement_status in 0s build + 0s test
TIMEOUT  src/engine.rs:2616:9: replace PPGEvaluator<T>::start_on_roots with () in 0s build + 20s test
