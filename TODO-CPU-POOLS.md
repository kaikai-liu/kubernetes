TODO:

* Our current usage of takeByTopology upon reconciling the
  configuration can easily have the exact opposite effect, IOW
  fragmenting allocations. We should roll a version instead which
  can take into account the set of cores we already have allocated
  and try to topologically optimise the combined full set of cores
  (as opposed to just the ones currently being allocated).

* While the current code keeps the original core / state / policy
  split, most of the duties from the original state has been lifted
  over to the pool code with state degenerating practically to act
  only as a layer for externally read-write locking the pool. The
  whole setup seems very artificially forced now. Maybe we should
  add locking to pool itself and get rid of using state altogether.
  Or alternatively change the layering so that any state-administration
  becomes hidden behind the Policy interface and the policy instances
  can freely decide what/which they use for maintaining permanent state.

* Add an 'external' policy which would allow one to run the logic of
  a CPUManager policy as en external entity (running as a pod). In
  its simplest form it would just relay AddContainer/RemoveContainer
  requests (together with the full pod/container spec) to the external
  entity over a channel, wait for the reply and deliver back to the
  caller.
