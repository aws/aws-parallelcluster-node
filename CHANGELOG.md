aws-parallelcluster-node CHANGELOG
===================================

This file is used to list changes made in each version of the aws-parallelcluster-node package.

2.2.1
-----

**CHANGES**
- `nodewatcher`: sge - improve logic to detect if a compute node has running jobs
- `sqswatcher`: remove invalid messages from SQS queue in order to process remaining messages
- `sqswatcher`: add number of slots to the log of torque scheduler
- `sqswatcher`: add retries in case aws request limits are reached

**BUG FIXES**
- `sqswatcher`: keep processing compute node termination until all scheduled jobs are terminated/cancelled.
  This allows to automatically remove dead nodes from the scheduler once all jobs are terminated.
- `jobwatcher`: better handling of error conditions and usage of fallback values
- `nodewatcher`: enable daemon when cluster status is `UPDATE_ROLLBACK_COMPLETE`

**TOOLING**
- Add a script to simplify node package upload when using `custom_node_package` option

2.1.1
-----

- China Regions, cn-north-1 and cn-northwest-1 support

2.1.0
-----

Bug Fixes:
- Don't schedule jobs on compute nodes that are terminating

2.0.2
-----

- Align version to main ParallelCluster package

2.0.0
-----

- Rename package to AWS ParallelCluster


1.6.0
-----

Bug fixes/minor improvements:

  - Changed scaling functionality to scale up and scale down faster.


1.5.4
-----

Bug fixes/minor improvements:

  - Upgraded Boto2 to Boto3 package.


1.5.2
-----

Bug fixes/minor improvements:

  - Fixed Slurm behavior to add CPU slots so multiple jobs can be scheduled on a single node, this also sets CPU as a consumable resource

1.5.1
-----

Bug fixes/minor improvements:

  - Fixed Torque behavior when scaling up from an empty cluster
  - Avoid Torque server restart when adding and removing compute nodes
