package sweforce.bdb.util;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.persist.EntityStore;

/**
 * Provides a shutdown hook specifying code to be run at the end of this application's life.
 * For more details, see: http://onjava.com/pub/a/onjava/2003/03/26/shutdownhook.html
 * @author Philip Johnson
 */
public class DbShutdownHook extends Thread {
  /** The environment instance to be closed upon shutdown. */
  private Environment env;
  /** The entity store to be closed upon shutdown. */
  private EntityStore store;

  /**
   * Called from within the ContactDAO to pass the Environment and EntityStore to be closed.
   * Note there can only be one of each of these in this application.
   * @param env The Environment.
   * @param store The EntityStore.
   */
  public DbShutdownHook(Environment env, EntityStore store) {
    this.env = env;
    this.store = store;
  }

  /**
   * Runs at system shutdown time and closes the environment and entity store.
   */
  public void run() {
    try {
      if (env != null) {
        store.close();
        env.cleanLog();
        env.close();
        // Comment this next line out during production, but good to see during development.
        System.out.println("Database closed.");
      }
    }
    catch (DatabaseException dbe) {
      System.out.println("Database not closed successfully.");
    }
  }
}