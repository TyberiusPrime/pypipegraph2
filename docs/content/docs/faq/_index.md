+++
title = "FAQ"
+++

## I'm defining jobs in a loop, but they all have the same output.

This is a common python pitfall. Python function definition only binds variables
the **first** time. You need to pass the variable as default parameters.

So instead of

```python
for ii, element in enumerate(whatever):
    ppg.FileGeneratingJob(str(ii) + '.txt', lambda of: of.write_text(element))
```

you need to use:
```python
for ii, element in enumerate(whatever):
    ppg.FileGeneratingJob(str(ii) + '.txt', lambda of, element=element: of.write_text(element))
```

to get the correct element.


