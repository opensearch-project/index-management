- [Developer Guide](#developer-guide)
    - [Forking and Cloning](#forking-and-cloning)
    - [Install Prerequisites](#install-prerequisites)
        - [JDK 21](#jdk-21)
    - [Setup](#setup)  
    - [Build](#build)
        - [Building from the command line](#building-from-the-command-line)
        - [Code Coverage](#code-coverage)
        - [Security Behavior Tests](#security-behavior-tests)
        - [Debugging](#debugging)
    - [Using IntelliJ IDEA](#using-intellij-idea)
    - [Submitting Changes](#submitting-changes)

## Developer Guide

So you want to contribute code to this project? Excellent! We're glad you're here. Here's what you need to do.

### Forking and Cloning

Fork this repository on GitHub, and clone locally with `git clone`.

### Install Prerequisites

#### JDK 21

OpenSearch components build using Java 21 at a minimum. This means you must have a JDK 21 installed with the environment variable `JAVA_HOME` referencing the path to Java home for your JDK 21 installation, e.g. `JAVA_HOME=/usr/lib/jvm/jdk-21`.

Download Java 21 from [here](https://adoptium.net/releases.html?variant=openjdk21).

## Setup

1. Check out this package from version control.
2. Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package.
3. To build from the command line, set `JAVA_HOME` to point to a JDK >= 21 before running `./gradlew`.
- Unix System
    1. `export JAVA_HOME=jdk-install-dir`: Replace `jdk-install-dir` with the JAVA_HOME directory of your system.
    2. `export PATH=$JAVA_HOME/bin:$PATH`

- Windows System
    1. Find **My Computers** from file directory, right click and select **properties**.
    2. Select the **Advanced** tab, select **Environment variables**.
    3. Edit **JAVA_HOME** to path of where JDK software is installed.


## Build

The project in this package uses the [Gradle](https://docs.gradle.org/current/userguide/userguide.html) build system. Gradle comes with excellent documentation that should be your first stop when trying to figure out how to operate or modify the build.

However, to build the `index management` plugin project, we also use the OpenSearch build tools for Gradle.  These tools are idiosyncratic and don't always follow the conventions and instructions for building regular Java code using Gradle. Not everything in `index management` will work the way it's described in the Gradle documentation. If you encounter such a situation, the OpenSearch build tools [source code](https://github.com/opensearch-project/OpenSearch/tree/main/buildSrc/src/main/groovy/org/opensearch/gradle) is your best bet for figuring out what's going on.

### Building from the command line

1. `./gradlew build` builds and tests project. 
2. `./gradlew run` launches a single node cluster with the index management (and job-scheduler) plugin installed.
3. `./gradlew run -PnumNodes=3` launches a multi-node cluster with the index management (and job-scheduler) plugin installed.
4. `./gradlew integTest` launches a single node cluster with the index management (and job-scheduler) plugin installed and runs all integ tests.
5. `./gradlew integTest -PnumNodes=3` launches a multi-node cluster with the index management (and job-scheduler) plugin installed and runs all integ tests.
6. `./gradlew integTest -Dtests.class=*RestChangePolicyActionIT` runs a single integ class
7. `./gradlew integTest -Dtests.class=*RestChangePolicyActionIT -Dtests.method="test missing index"` runs a single integ test method (remember to quote the test method name if it contains spaces)
8. `./gradlew indexmanagementBwcCluster#mixedClusterTask -Dtests.security.manager=false` launches a cluster of three nodes of bwc version of OpenSearch with index management and tests backwards compatibility by performing rolling upgrade of each node with the current version of OpenSearch with index management.
9. `./gradlew indexmanagementBwcCluster#rollingUpgradeClusterTask -Dtests.security.manager=false` launches a cluster with three nodes of bwc version of OpenSearch with index management and tests backwards compatibility by performing rolling upgrade of each node with the current version of OpenSearch with index management.
10. `./gradlew indexmanagementBwcCluster#fullRestartClusterTask -Dtests.security.manager=false` launches a cluster with three nodes of bwc version of OpenSearch with index management and tests backwards compatibility by performing a full restart on the cluster upgrading all the nodes with the current version of OpenSearch with index management.
11. `./gradlew bwcTestSuite -Dtests.security.manager=false` runs all the above bwc tests combined.
12. `./gradlew integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername="docker-cluster" -Dhttps=true -Duser=admin -Dpassword=admin` launches integration tests against a local cluster and run tests with security
13. `./gradlew integTest -Dsecurity=true -Dhttps=true --tests '*SecurityBehaviorIT'` runs all security behavior tests with security enabled

When launching a cluster using one of the above commands, logs are placed in `build/testclusters/integTest-0/logs`. Though the logs are teed to the console, in practices it's best to check the actual log file.

### Code Coverage

The project supports generating code coverage reports using JaCoCo.

#### Generating Coverage Reports

To generate code coverage reports

```bash
./gradlew check -Dtests.coverage=true
```

Or run any tests, and generate test report

```bash
./gradlew test
./gradlew jacocoTestReport
```

#### Coverage Report Locations

After running with coverage enabled, reports are generated in:
- **HTML Report**: `build/reports/jacoco/test/html/index.html` (human readable)
- **XML Report**: `build/reports/jacoco/test/jacocoTestReport.xml` (for tools like Codecov)

### Security Behavior Tests

Security behavior tests ensure that the Index Management plugin properly enforces access controls and permissions. These tests validate that users can only perform operations they are authorized for and receive appropriate error responses when access is denied.

#### Overview

Security behavior tests extend the `SecurityRestTestCase` base class and test various permission scenarios:
- API endpoint permission enforcement
- Policy based authentication and authorization

#### Running Security Tests

Security tests require additional flags and must be run against a cluster with security enabled:

```bash
# Run all security behavior tests
./gradlew integTest -Dsecurity=true -Dhttps=true --tests '*SecurityBehaviorIT'

# Run a specific security test class
./gradlew integTest -Dsecurity=true -Dhttps=true --tests '*PolicySecurityBehaviorIT'

# Run security tests against a remote cluster with authentication
./gradlew integTestRemote -Dtests.rest.cluster=localhost:9200 -Dtests.cluster=localhost:9200 -Dtests.clustername="docker-cluster" -Dhttps=true -Duser=admin -Dpassword=admin
```

#### Writing New Security Tests

When adding new security tests, follow these guidelines:

1. **Extend SecurityRestTestCase**: All security tests should extend this base class
2. **Test both success and failure scenarios**: Verify authorized access works and unauthorized access is properly denied
3. **Use appropriate HTTP status codes**: Expect 401 (Unauthorized) or 403 (Forbidden) for denied access
4. **Test role-based permissions**: Create users with specific roles and test their access levels
5. **Include cleanup**: Ensure users and roles created during tests are properly cleaned up

Example test structure:
```kotlin
@TestLogging("level:DEBUG", reason = "Debug for tests.")
class MyFeatureSecurityBehaviorIT : SecurityRestTestCase() {
    
    @Before
    fun setupUsersAndRoles() {
        // Create test users and roles
    }
    
    @After  
    fun cleanup() {
        // Clean up test users and roles
    }
    
    fun testAuthorizedAccess() {
        // Test that authorized users can perform operations
    }
    
    fun testUnauthorizedAccess() {
        // Test that unauthorized users receive proper error responses
    }
}
```

### Debugging

Sometimes it is useful to attach a debugger to either the OpenSearch cluster or the integ tests to see what's going on. When running unit tests, hit **Debug** from the IDE's gutter to debug the tests.  For the OpenSearch cluster or the integ tests, first, make sure start a debugger listening on port `5005`.

To debug the server code, run:

```
./gradlew :integTest -Dcluster.debug # to start a cluster with debugger and run integ tests
```

OR

```
./gradlew run --debug-jvm # to just start a cluster that can be debugged
```

The OpenSearch server JVM will connect to a debugger attached to `localhost:5005`.

The IDE needs to listen for the remote JVM. If using Intellij you must set your debug configuration to "Listen to remote JVM" and make sure "Auto Restart" is checked.
You must start your debugger to listen for remote JVM before running the commands.

To debug code running in an integration test (which exercises the server from a separate JVM), first, setup a remote debugger listening on port `8000`, and then run:

```
./gradlew :integTest -Dtest.debug
```

The test runner JVM will connect to a debugger attached to `localhost:8000` before running the tests.

Additionally, it is possible to attach one debugger to the cluster JVM and another debugger to the test runner. First, make sure one debugger is listening on port `5005` and the other is listening on port `8000`. Then, run:
```
./gradlew :integTest -Dtest.debug -Dcluster.debug
```

### Using IntelliJ IDEA

Launch Intellij IDEA, choose **Import Project**, and select the `settings.gradle` file in the root of this package.

### Submitting Changes

See [CONTRIBUTING](CONTRIBUTING.md).

### Backport

- [Link to backport documentation](https://github.com/opensearch-project/opensearch-plugins/blob/main/BACKPORT.md)
