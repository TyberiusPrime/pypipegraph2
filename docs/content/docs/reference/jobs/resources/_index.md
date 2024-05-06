+++
title = "Resources enum"
+++

What resources a job needs.
Possible values:

 * ppg.Resources.SingleCore - Start as many as you have cores (see ppg.new)
 * ppg.Resources.AllCores - all cores, but run one SingleCore job in parallel
 * ppg.Resources.Exclusive - really use all cores.
