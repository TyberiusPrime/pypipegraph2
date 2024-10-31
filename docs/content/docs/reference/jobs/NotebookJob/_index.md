+++
title= "NotebookJob"
Weight= 40
+++

# ppg2.NotebookJob

```python
NotebookJob(
    notebook_file: Path, 
    input_files: [Path], 
    output_files: [Path], 
    html_output_folder: Path)
```


Run a jupyter notebook, tracking input and output files. 

Then tebook is rendered into an html file in html_output_folder.

Reruns whenever the *code* of the notebook changes, not the notebook-contained results.

