+++
title= "PlotJob"
Weight= 41
+++

# ppg2.PlotJob

```python
PlotJob(  # noqa:C901
    output_filename,
    calc_function,
    plot_function,
    render_args=None,
    cache_dir="cache",
    depend_on_function=True,
    cache_calc=True,
    create_table=True,
)  # noqa:C901
```

Create a plot from a calculating function (meant to return a DataFrame)
and a plot function (that consumes that dataframe).

The plot_function may return any object that has a savefig/render/save method,
such as matplotlib plots or plotnine ggplots.

The calc_function is called with no arguments, and is expected to return a pandas.DataFrame
or  a dict of such.

Calculatinons might be cached ( in `cache/<output_filename>`),
to speed up iteration when tuning the plot.

Optionaly, a `<output_filename + '.tsv'>` file is created with the data used for the plot.
(If calc returns a dictionary, they are concated into one file, with \# comments between the 
elements).


The return value of this function is a named tuple (plot, cache, table) containing the three jobs created.
(If cache_calc is False, cache will be None, if create_table is False, table will be None).

