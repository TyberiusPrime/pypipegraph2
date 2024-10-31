+++
title = "UseInputHashesForOutput"
+++

ppg.UseInputHashesForOutput

```python
ppg2.UseInputHashesForOutput(payload = None)
```

For [DataLoadingJobs](../dataloadingjob/) and [AttributeLoadingJobs](../attributeloadingjob/), you can return a `ppg2.UseInputHashesForOutput(payload = None)` object. This signals the ppg to use the jobs' input hashes to derive it's [tracking hash](../concepts/tracking-hash).

You can use this if your object/payload is unsupported by [DeepDiff](https://pypi.org/project/deepdiff/), or simply strictly dependend
on it's inputs.


