aws-parallelcluster-node CHANGELOG
===================================

This file is used to list changes made in each version of the aws-parallelcluster-node package.

2.2.0
-----
- Prevent jobwatcher crashes by using fallback values
- Improve nodewatcher logging
- sqswatcher: add number of slots to the log of torque scheduler
- Improved function to test if a compute node has running jobs
- Add retries in case aws request limits are reached
- Remove invalid messages from SQS queue in order to process remaining messages
- Add upload node script `util/uploadNode.sh`

Minor:
- Add `UPDATE_ROLLBACK_COMPLETE` to stack completion statuses

2.1.0
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
