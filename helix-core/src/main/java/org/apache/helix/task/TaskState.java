/*
 * $Id$
 */
package org.apache.helix.task;


/**
 * Enumeration of current task states. This value is stored in the rebalancer context.
 *
 * @author Abe <asebasti@linkedin.com>
 * @version $Revision$
 */
public enum TaskState
{
  /**
   * The task is in progress.
   */
  IN_PROGRESS,
  /**
   * The task has been stopped. It may be resumed later.
   */
  STOPPED,
  /**
   * The task has failed. It cannot be resumed.
   */
  FAILED,
  /**
   * All the task partitions have completed normally.
   */
  COMPLETED
}
