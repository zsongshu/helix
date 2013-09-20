/*
 * $Id$
 */
package org.apache.helix.task;


/**
 * A factory for {@link Task} objects.
 *
 * @author Abe <asebasti@linkedin.com>
 * @version $Revision$
 */
public interface TaskFactory
{
  /**
   * Returns a {@link Task} instance.
   *
   * @param config Configuration information for the task.
   *
   * @return A {@link Task} instance.
   */
  Task createNewTask(String config);
}
