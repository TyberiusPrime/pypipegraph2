# Error messages are all bad.
They have no user actionable stuff.

example: Hash mismatch on a fetchjob.
```
    print(ppg3.run())
          ~~~~~~~~^^
  File "/home/finkernagel/upstream/pypipegraph2/ppg3/python/ppg3/run.py", line 347, in run
    raise PPGRunError(RunResult(report, generation=None))
ppg3.run.PPGRunError: ppg3 run: 1 job(s) failed: 'incoming/test.dat': job failed: job "incoming/test.dat" exited with code 1
--- stderr tail ---
ppg3._shim: fetch hash mismatch for https://raw.githubusercontent.com/TyberiusPrime/fastqrab/refs/heads/main/README.md: expected 8b4e5da92263fa998f65a7692ed9dbc09e4b89a88e59c5b5ca07fb02c1ecadc6, got d08ed3d7214ea58d3091be4e67d70547b48692234bb3d9ef7c93d57b4b8d7514
```

Good: We got the url. Bad: Neither the store/entries we're comparing against,
nor the downloaded version is present.



---
ppg3.run()
    On any job failure, the view is left untouched (§11: a generation must
    be a consistent, all-or-nothing snapshot) and :class:`PPGRunError` is
    raised with the full report attached.

That's awful. The user wants to inspect job outputs as they're done,
not after a multi day run...


And the report sucks.

---

Jobs creating the same target views are in conflict once you add in a name,
and the error is bad, and I don't even think that 'name=' should be a thing.


-- 
rollback ux is shit. 
It needs at least to print how to get back.

It needs to be able to produce the link tree in a different folder.

-- 
where is the 'materialize' command that turns an output folder into 
a non-symlinked copy?

-- 
ux generations:
'created_at_ms' - user facing timestamps? seriously?

--
explain should list the diff-entries command.
diff enries should offer to actually diff the damn files...

-- 
gc does nothing. even after removing all the generations

--
wtf is meta.json, what's the use for the user?

--
ppg3 binary
auto find the store if there is only one.


-- 
how do I even change the output folder?

--
failing command jobs are only detected because of missing output?


-- 
fetch jobs fail getting the same file again?

ppg3.run.PPGRunError: ppg3 run: 2 job(s) failed: 'incoming/test.dat': job
failed: job "incoming/test.dat" declared fixed_output
"d08ed3d7214ea58d3091be4e67d70547b48692234bb3d9ef7c93d57b4b8d7514" but produced
"676bd2f93c9d33656e56cd62c7fa28a4839393ecd82fd86ae4cc8c5be3eee55d", 'shunk':
upstream failed: incoming/test.dat

and I can't find that first output anywhere? That's from the TOFU though
