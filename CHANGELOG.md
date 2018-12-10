cfncluster-node CHANGELOG
=========================

This file is used to list changes made in each version of the cfncluster-node package.

1.6.1
-----

Bug fixes:

  - Changed scaling functionality to prevent scheduling jobs on dead nodes.
  - Fixed a bug where scale down operations were not performed on clusters which were updated


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
