# Testing

Havenask uses jUnit for testing, it also uses randomness in the
tests, that can be set using a seed, the following is a cheatsheet of
options for running the tests for Havenask.

# Creating packages

To create a distribution without running the tests, run the following:

    ./gradlew assemble

To create a platform-specific build, use the following depending on your operating system:

    ./gradlew :distribution:archives:linux-tar:assemble
    ./gradlew :distribution:archives:darwin-tar:assemble
    ./gradlew :distribution:archives:windows-zip:assemble

## Running Havenask from a checkout

In order to run Havenask from source without building a package, you can run it using Gradle:

    ./gradlew run

### Launching and debugging from an IDE

**NOTE:** If you have imported the project into IntelliJ according to the instructions in [DEVELOPER_GUIDE.md](DEVELOPER_GUIDE.md#importing-the-project-into-intellij-idea), then a debug run configuration named **Debug Havenask** will be created for you and configured appropriately.

To run Havenask in debug mode,

1. Start the `Debug Havenask` in IntelliJ by pressing the debug icon.
2. From a terminal run the following `./gradlew run --debug-jvm`. You can also run this task in IntelliJ.

This will instruct all JVMs (including any that run cli tools such as creating the keyring or adding users) to suspend and initiate a debug connection on port incrementing from `5005`. As such, the IDE needs to be instructed to listen for connections on this port. Since we might run multiple JVMs as part of configuring and starting the cluster, it's recommended to configure the IDE to initiate multiple listening attempts. In case of IntelliJ, this option is called "Auto restart" and needs to be checked. In case of Eclipse, "Connection limit" setting needs to be configured with a greater value (ie 10 or more).

### Other useful arguments

-   In order to start a node with a different max heap space add: `-Dtests.heap.size=4G`
-   In order to disable assertions add: `-Dtests.asserts=false`
-   In order to use a custom data directory: `--data-dir=/tmp/foo`
-   In order to preserve data in between executions: `--preserve-data`
-   In order to remotely attach a debugger to the process: `--debug-jvm`
-   In order to set a different keystore password: `--keystore-password yourpassword`
-   In order to set an Havenask setting, provide a setting with the following prefix: `-Dtests.havenask.`

## Test case filtering

-   `tests.class` is a class-filtering shell-like glob pattern
-   `tests.method` is a method-filtering glob pattern.

Run a single test case (variants)

    ./gradlew test -Dtests.class=org.havenask.package.ClassName
    ./gradlew test "-Dtests.class=*.ClassName"

Run all tests in a package and its sub-packages

    ./gradlew test "-Dtests.class=org.havenask.package.*"

Run any test methods that contain *esi* (e.g.: .r*esi*ze.)

    ./gradlew test "-Dtests.method=*esi*"

Run all tests that are waiting for a bugfix (disabled by default)

    ./gradlew test -Dtests.filter=@awaitsfix

## Seed and repetitions

Run with a given seed (seed is a hex-encoded long).

    ./gradlew test -Dtests.seed=DEADBEEF

## Repeats *all* tests of ClassName N times

Every test repetition will have a different method seed (derived from a single random master seed).

    ./gradlew test -Dtests.iters=N -Dtests.class=*.ClassName

## Repeats *all* tests of ClassName N times

Every test repetition will have exactly the same master (0xdead) and method-level (0xbeef) seed.

    ./gradlew test -Dtests.iters=N -Dtests.class=*.ClassName -Dtests.seed=DEAD:BEEF

## Repeats a given test N times

Note that individual test repetitions are passed suffixes, such as: `testFoo[0]`, `testFoo[1]`, etc. Thus using `testmethod` or `tests.method` ending in a glob is necessary to ensure iterations are run.

    ./gradlew test -Dtests.iters=N -Dtests.class=*.ClassName -Dtests.method=mytest*

Repeats N times but skips any tests after the first failure or M initial failures.

    ./gradlew test -Dtests.iters=N -Dtests.failfast=true -Dtestcase=...
    ./gradlew test -Dtests.iters=N -Dtests.maxfailures=M -Dtestcase=...

## Test groups

Test groups can be enabled or disabled (true/false).

Default value provided below in \[brackets\].

    ./gradlew test -Dtests.awaitsfix=[false] - known issue (@AwaitsFix)

## Load balancing and caches

By default, the tests run on multiple processes using all the available cores on all available CPUs. Not including hyper-threading. If you want to explicitly specify the number of JVMs you can do so on the command line:

    ./gradlew test -Dtests.jvms=8

Or in `~/.gradle/gradle.properties`:

    systemProp.tests.jvms=8

It's difficult to pick the "right" number here. Hypercores don’t count for CPU intensive tests, and you should leave some slack for JVM-internal threads like the garbage collector. And you have to have enough RAM to handle each JVM.

## Test compatibility

It is possible to provide a version that allows to adapt the tests' behaviour to older features or bugs that have been changed or fixed in the meantime.

    ./gradlew test -Dtests.compatibility=1.0.0

## Miscellaneous

Run all tests without stopping on errors (inspect log files).

    ./gradlew test -Dtests.haltonfailure=false

Run more verbose output (JVM parameters, etc.).

    ./gradlew test -verbose

Change the default suite timeout to 5 seconds for all tests (note the exclamation mark).

    ./gradlew test -Dtests.timeoutSuite=5000! ...

Change the logging level of Havenask (not Gradle)

    ./gradlew test -Dtests.havenask.logger.level=DEBUG

Print all the logging output from the test runs to the commandline even if tests are passing.

    ./gradlew test -Dtests.output=true

Configure the heap size.

    ./gradlew test -Dtests.heap.size=512m

Pass arbitrary jvm arguments.

    # specify heap dump path
    ./gradlew test -Dtests.jvm.argline="-XX:HeapDumpPath=/path/to/heapdumps"
    # enable gc logging
    ./gradlew test -Dtests.jvm.argline="-verbose:gc"
    # enable security debugging
    ./gradlew test -Dtests.jvm.argline="-Djava.security.debug=access,failure"

# Running verification tasks

To run all verification tasks, including static checks, unit tests, and integration tests:

    ./gradlew check

Note that this will also run the unit tests and precommit tasks first. If you want to just run the in memory cluster integration tests (because you are debugging them):

    ./gradlew internalClusterTest

If you want to just run the precommit checks:

    ./gradlew precommit

Some of these checks will require `docker-compose` installed for bringing up test fixtures. If it’s not present those checks will be skipped automatically.

# Testing the REST layer

The REST layer is tested through specific tests that are executed against a cluster that is configured and initialized via Gradle. The tests themselves can be written in either Java or with a YAML based DSL.

YAML based REST tests should be preferred since these are shared between clients. The YAML based tests describe the operations to be executed, and the obtained results that need to be tested.

The YAML tests support various operators defined in the [rest-api-spec](/rest-api-spec/src/main/resources/rest-api-spec/test/README.md) and adhere to the [Havenask REST API JSON specification](/rest-api-spec/README.md). In order to run the YAML tests, the relevant API specification needs to be on the test classpath. Any gradle project that has support for REST tests will get the primary API on it’s class path. However, to better support Gradle incremental builds, it is recommended to explicitly declare which parts of the API the tests depend upon.

For example:

    restResources {
      restApi {
        includeCore '_common', 'indices', 'index', 'cluster', 'nodes', 'get', 'ingest'
      }
    }

The REST tests are run automatically when executing the "./gradlew check" command. To run only the YAML REST tests use the following command (modules and plugins may also include YAML REST tests):

    ./gradlew :rest-api-spec:yamlRestTest

A specific test case can be run with the following command:

    ./gradlew ':rest-api-spec:yamlRestTest' \
      --tests "org.havenask.test.rest.ClientYamlTestSuiteIT" \
      -Dtests.method="test {p0=cat.segments/10_basic/Help}"

The YAML REST tests support all the options provided by the randomized runner, plus the following:

-   `tests.rest.suite`: comma separated paths of the test suites to be run (by default loaded from /rest-api-spec/test). It is possible to run only a subset of the tests providing a sub-folder or even a single yaml file (the default /rest-api-spec/test prefix is optional when files are loaded from classpath) e.g. `-Dtests.rest.suite=index,get,create/10_with_id`

-   `tests.rest.blacklist`: comma separated globs that identify tests that are blacklisted and need to be skipped e.g. `-Dtests.rest.blacklist=index/**/Index document,get/10_basic/**`

Java REST tests can be run with the "javaRestTest" task.

For example :

    ./gradlew :modules:mapper-extras:javaRestTest

    ./gradlew ':modules:mapper-extras:javaRestTest' \
      --tests "org.havenask.index.mapper.TokenCountFieldMapperIntegrationIT.testSearchByTokenCount {storeCountedFields=true loadCountedFields=false}"

yamlRestTest’s and javaRestTest’s are easy to identify, since they are found in a respective source directory. However, there are some more specialized REST tests that use custom task names. These are usually found in "qa" projects commonly use the "integTest" task.

If in doubt about which command to use, simply run &lt;gradle path&gt;:check

Note that the REST tests, like all the integration tests, can be run against an external cluster by specifying the `tests.cluster` property, which if present needs to contain a comma separated list of nodes to connect to (e.g. localhost:9300). A transport client will be created based on that and used for all the before|after test operations, and to extract the http addresses of the nodes so that REST requests can be sent to them.

# Testing packaging

The packaging tests use Vagrant virtual machines or cloud instances to verify that installing and running Havenask distributions works correctly on supported operating systems. These tests should really only be run on ephemeral systems because they’re destructive; that is, these tests install and remove packages and freely modify system settings, so you will probably regret it if you execute them on your development machine.

When you run a packaging test, Gradle will set up the target VM and mount your repository directory in the VM. Once this is done, a Gradle task will issue a Vagrant command to run a **nested** Gradle task on the VM. This nested Gradle runs the actual "destructive" test classes.

1.  Install Virtual Box and Vagrant.

2.  (Optional) Install [vagrant-cachier](https://github.com/fgrehm/vagrant-cachier) to squeeze a bit more performance out of the process (Note: as of 2021, vagrant-cachier is unmaintained):

        vagrant plugin install vagrant-cachier

3.  You can run all the OS packaging tests with `./gradlew packagingTest`. This task includes our legacy `bats` tests.

    To run only the OS tests that are written in Java, run `.gradlew distroTest`, will cause Gradle to build the tar, zip, and deb packages and all the plugins. It will then run the tests on every available system. This will take a very long time.

    Fortunately, the various systems under test have their own Gradle tasks under `qa/os`. To find out what packaging combinations can be tested on a system, run the `tasks` task. For example:

        ./gradlew :qa:os:ubuntu-1804:tasks

    If you want a quick test of the tarball and RPM packaging for Centos 7, you would run:

        ./gradlew :qa:os:centos-7:distroTest.rpm :qa:os:centos-7:distroTest.linux-archive

Note that if you interrupt Gradle in the middle of running these tasks, any boxes started will remain running, and you’ll have to stop them manually with `./gradlew --stop` or `vagrant halt`.

All the regular vagrant commands should just work, so you can get a shell in a VM running trusty by running `vagrant up ubuntu-1604 --provider virtualbox && vagrant ssh ubuntu-1604`.

These are the linux flavors supported, all of which we provide images for

-   ubuntu-1604 aka xenial
-   ubuntu-1804 aka bionic beaver
-   debian-8 aka jessie
-   debian-9 aka stretch, the current debian stable distribution
-   centos-6
-   centos-7
-   rhel-8
-   fedora-28
-   fedora-29
-   oel-6 aka Oracle Enterprise Linux 6
-   oel-7 aka Oracle Enterprise Linux 7
-   sles-12
-   opensuse-42 aka Leap

We’re missing the following from the support matrix because there are no high quality boxes available in vagrant atlas:

-   sles-11

## Testing packaging on Windows

The packaging tests also support Windows Server 2012R2 and Windows Server 2016. Unfortunately we’re not able to provide boxes for them in open source use because of licensing issues. Any Virtualbox image that has WinRM and Powershell enabled for remote users should work.

Testing on Windows requires the [vagrant-winrm](https://github.com/criteo/vagrant-winrm) plugin.

    vagrant plugin install vagrant-winrm

Specify the image IDs of the Windows boxes to gradle with the following project properties. They can be set in `~/.gradle/gradle.properties` such as

    vagrant.windows-2012r2.id=my-image-id
    vagrant.windows-2016.id=another-image-id

or passed on the command line such as `-Pvagrant.windows-2012r2.id=my-image-id` or `-Pvagrant.windows-2016=another-image-id`

These properties are required for Windows support in all gradle tasks that handle packaging tests. Either or both may be specified.

If you’re running vagrant commands outside of gradle, specify the Windows boxes with the environment variables.

-   `VAGRANT_WINDOWS_2012R2_BOX`
-   `VAGRANT_WINDOWS_2016_BOX`

## Testing VMs are disposable

It’s important to think of VMs like cattle. If they become lame you just shoot them and let vagrant reprovision them. Say you’ve hosed your precise VM:

    vagrant ssh ubuntu-1604 -c 'sudo rm -rf /bin'; echo oops

All you’ve got to do to get another one is

    vagrant destroy -f ubuntu-1604 && vagrant up ubuntu-1604 --provider virtualbox

The whole process takes a minute and a half on a modern laptop, two and a half without vagrant-cachier.

Some vagrant commands will work on all VMs at once:

    vagrant halt
    vagrant destroy -f

`vagrant up` would normally start all the VMs, but we’ve prevented that because that’d consume a ton of ram.

## Iterating on packaging tests

Because our packaging tests are capable of testing many combinations of OS (e.g., Windows, Linux, etc.), package type (e.g., zip file, RPM, etc.) and so forth, it’s faster to develop against smaller subsets of the tests. For example, to run tests for the default archive distribution on Fedora 28:

    ./gradlew :qa:os:fedora-28:distroTest.linux-archive

These test tasks can use the `--tests`, `--info`, and `--debug` parameters just like non-OS tests can. For example:

    ./gradlew :qa:os:fedora-28:distroTest.linux-archive --tests "com.havenask.packaging.test.ArchiveTests"

# Testing backwards compatibility

Backwards compatibility tests exist to test upgrading from each supported version to the current version. To run them all use:

    ./gradlew bwcTest

A specific version can be tested as well. For example, to test bwc with version 5.3.2 run:

    ./gradlew v5.3.2#bwcTest

Use -Dtest.class and -Dtests.method to run a specific bwcTest test. For example to run a specific tests from the x-pack rolling upgrade from 7.7.0:

    ./gradlew :x-pack:qa:rolling-upgrade:v7.7.0#bwcTest \
     -Dtests.class=org.havenask.upgrades.UpgradeClusterClientYamlTestSuiteIT \
     -Dtests.method="test {p0=*/40_ml_datafeed_crud/*}"

Tests are run for versions that are not yet released but with which the current version will be compatible with. These are automatically checked out and built from source. See [VersionCollection](./buildSrc/src/main/java/org/havenask/gradle/VersionCollection.java) and [distribution/bwc/build.gradle](./distribution/bwc/build.gradle) for more information.

When running `./gradlew check`, minimal bwc checks are also run against compatible versions that are not yet released.

## BWC Testing against a specific remote/branch

Sometimes a backward compatibility change spans two versions. A common case is a new functionality that needs a BWC bridge in an unreleased versioned of a release branch (for example, 5.x). To test the changes, you can instruct Gradle to build the BWC version from a another remote/branch combination instead of pulling the release branch from GitHub. You do so using the `bwc.remote` and `bwc.refspec.BRANCH` system properties:

    ./gradlew check -Dbwc.remote=${remote} -Dbwc.refspec.5.x=index_req_bwc_5.x

The branch needs to be available on the remote that the BWC makes of the repository you run the tests from. Using the remote is a handy trick to make sure that a branch is available and is up to date in the case of multiple runs.

Example:

Say you need to make a change to `master` and have a BWC layer in `5.x`. You will need to: . Create a branch called `index_req_change` off your remote `${remote}`. This will contain your change. . Create a branch called `index_req_bwc_5.x` off `5.x`. This will contain your bwc layer. . Push both branches to your remote repository. . Run the tests with `./gradlew check -Dbwc.remote=${remote} -Dbwc.refspec.5.x=index_req_bwc_5.x`.

### Skip fetching latest

For some BWC testing scenarios, you want to use the local clone of the repository without fetching latest. For these use cases, you can set the system property `tests.bwc.git_fetch_latest` to `false` and the BWC builds will skip fetching the latest from the remote.

# How to write good tests?

## Base classes for test cases

There are multiple base classes for tests:

-   **`HavenaskTestCase`**: The base class of all tests. It is typically extended directly by unit tests.
-   **`HavenaskSingleNodeTestCase`**: This test case sets up a cluster that has a single node.
-   **`HavenaskIntegTestCase`**: An integration test case that creates a cluster that might have multiple nodes.
-   **`HavenaskRestTestCase`**: An integration tests that interacts with an external cluster via the REST API. This is used for Java based REST tests.
-   **`HavenaskClientYamlSuiteTestCase`** : A subclass of `HavenaskRestTestCase` used to run YAML based REST tests.

## Good practices

### What kind of tests should I write?

Unit tests are the preferred way to test some functionality: most of the time they are simpler to understand, more likely to reproduce, and unlikely to be affected by changes that are unrelated to the piece of functionality that is being tested.

The reason why `HavenaskSingleNodeTestCase` exists is that all our components used to be very hard to set up in isolation, which had led us to having a number of integration tests but close to no unit tests. `HavenaskSingleNodeTestCase` is a workaround for this issue which provides an easy way to spin up a node and get access to components that are hard to instantiate like `IndicesService`. Whenever practical, you should prefer unit tests.

Many tests extend `HavenaskIntegTestCase`, mostly because this is how most tests used to work in the early days of Elasticsearch. However, the complexity of these tests tends to make them hard to debug. Whenever the functionality that is being tested isn’t intimately dependent on how Havenask behaves as a cluster, it is recommended to write unit tests or REST tests instead.

In short, most new functionality should come with unit tests, and optionally REST tests to test integration.

### Refactor code to make it easier to test

Unfortunately, a large part of our code base is still hard to unit test. Sometimes because some classes have lots of dependencies that make them hard to instantiate. Sometimes because API contracts make tests hard to write. Code refactors that make functionality easier to unit test are encouraged. If this sounds very abstract to you, you can have a look at [this pull request](https://github.com/elastic/elasticsearch/pull/16610) for instance, which is a good example. It refactors `IndicesRequestCache` in such a way that: - it no longer depends on objects that are hard to instantiate such as `IndexShard` or `SearchContext`, - time-based eviction is applied on top of the cache rather than internally, which makes it easier to assert on what the cache is expected to contain at a given time.

## Bad practices

### Use randomized-testing for coverage

In general, randomization should be used for parameters that are not expected to affect the behavior of the functionality that is being tested. For instance the number of shards should not impact `date_histogram` aggregations, and the choice of the `store` type (`niofs` vs `mmapfs`) does not affect the results of a query. Such randomization helps improve confidence that we are not relying on implementation details of one component or specifics of some setup.

However, it should not be used for coverage. For instance if you are testing a piece of functionality that enters different code paths depending on whether the index has 1 shards or 2+ shards, then we shouldn’t just test against an index with a random number of shards: there should be one test for the 1-shard case, and another test for the 2+ shards case.

### Abuse randomization in multi-threaded tests

Multi-threaded tests are often not reproducible due to the fact that there is no guarantee on the order in which operations occur across threads. Adding randomization to the mix usually makes things worse and should be done with care.

# Test coverage analysis

Generating test coverage reports for Havenask is currently not possible through Gradle. However, it *is* possible to gain insight in code coverage using IntelliJ’s built-in coverage analysis tool that can measure coverage upon executing specific tests. Eclipse may also be able to do the same using the EclEmma plugin.

Test coverage reporting used to be possible with JaCoCo when Havenask was using Maven as its build system. Since the switch to Gradle though, this is no longer possible, seeing as the code currently used to build Havenask does not allow JaCoCo to recognize its tests. For more information on this, see the discussion in [issue #28867](https://github.com/elastic/elasticsearch/issues/28867).

Read your IDE documentation for how to attach a debugger to a JVM process.

# Building with extra plugins

Additional plugins may be built alongside Havenask, where their dependency on Havenask will be substituted with the local Havenask build. To add your plugin, create a directory called havenask-extra as a sibling of Havenask. Checkout your plugin underneath havenask-extra and the build will automatically pick it up. You can verify the plugin is included as part of the build by checking the projects of the build.

    ./gradlew projects

# Environment misc

There is a known issue with macOS localhost resolve strategy that can cause some integration tests to fail. This is because integration tests have timings for cluster formation, discovery, etc. that can be exceeded if name resolution takes a long time. To fix this, make sure you have your computer name (as returned by `hostname`) inside `/etc/hosts`, e.g.:

    127.0.0.1       localhost HavenaskMBP.local
    255.255.255.255 broadcasthost
    ::1             localhost HavenaskMBP.local`

# Benchmarking

For changes that might affect the performance characteristics of Havenask you should also run macrobenchmarks. There is also a macrobenchmarking tool called [Rally](https://github.com/elastic/rally) which you can use to measure the performance impact. To get started, please see [Rally’s documentation](https://esrally.readthedocs.io/en/stable/).
