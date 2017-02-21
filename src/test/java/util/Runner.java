package util;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

public class Runner {

    private static final String PROJECT_DIR = "vertx-redis-cluster";
    private static final String PROJECT_JAVA_DIR = PROJECT_DIR + "/src/main/java/";

    public static void runClustered(Class clazz) {
        run(PROJECT_JAVA_DIR, clazz, new VertxOptions().setClustered(true), null);
    }

    public static void run(Class clazz) {
        run(PROJECT_JAVA_DIR, clazz, new VertxOptions().setClustered(false), null);
    }

    public static void run(Class clazz, DeploymentOptions options) {
        run(PROJECT_JAVA_DIR, clazz, new VertxOptions().setClustered(false), options);
    }

    public static void run(String dir, Class clazz, VertxOptions options, DeploymentOptions
            deploymentOptions) {
        run(dir + clazz.getPackage().getName().replace(".", "/"), clazz.getName(), options, deploymentOptions);
    }

    public static void runScript(String prefix, String scriptName, VertxOptions options) {
        File file = new File(scriptName);
        String dirPart = file.getParent();
        String scriptDir = prefix + dirPart;
        run(scriptDir, scriptDir + "/" + file.getName(), options, null);
    }

    public static void run(String dir, String verticleID, VertxOptions options, DeploymentOptions deploymentOptions) {
        if (options == null) {
            options = new VertxOptions();
        }
        try {
            // We need to use the canonical file. Without the file name is .
            File current = new File(".").getCanonicalFile();
            if (dir.startsWith(current.getName()) && !dir.equals(current.getName())) {
                dir = dir.substring(current.getName().length() + 1);
            }
        } catch (IOException e) {
            // Ignore it.
        }

        System.setProperty("vertx.cwd", dir);
        Consumer<Vertx> runner = vertx -> {
            try {
                if (deploymentOptions != null) {
                    vertx.deployVerticle(verticleID, deploymentOptions);
                } else {
                    vertx.deployVerticle(verticleID);
                }
            } catch (Throwable t) {
                t.printStackTrace();
            }
        };
        if (options.isClustered()) {
            Vertx.clusteredVertx(options, res -> {
                if (res.succeeded()) {
                    Vertx vertx = res.result();
                    runner.accept(vertx);
                } else {
                    res.cause().printStackTrace();
                }
            });
        } else {
            Vertx vertx = Vertx.vertx(options);
            runner.accept(vertx);
        }
    }
}