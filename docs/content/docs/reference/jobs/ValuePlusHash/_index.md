+++
title = "ValuePlusHash"
+++

# ValuePlusHash


In situations where a job must return an Object, but you already have a hash handy
(or need to provide one due to DeepDiff not supporting your object type), 
you can return a `ppg2.ValuePlusHash(payload, hash_hexdigest)` object.

This is supported by [AttributeLoadingJobs](../AttributeLoadingJob) and [DataLoadingJobs](../DataLoadingJob),
though the later technically do not make use of the payload.

It is not used by  CachedAttributeLoadingJob or CachedDataLoadingJobs - these use the pickled representation
that their caching part uses to calculate the hash. They use ValuePlusHash internally.

At times, returning a [UseInputHashesForOutput(payload)](../UseInputHashesForOutput) use might be more appropriate.
