set SCRIPT=%0

rem determine Havenask home; to do this, we strip from the path until we
rem find bin, and then strip bin (there is an assumption here that there is no
rem nested directory under bin also named bin)
for %%I in (%SCRIPT%) do set HAVENASK_HOME=%%~dpI

:havenask_home_loop
for %%I in ("%HAVENASK_HOME:~1,-1%") do set DIRNAME=%%~nxI
if not "%DIRNAME%" == "bin" (
  for %%I in ("%HAVENASK_HOME%..") do set HAVENASK_HOME=%%~dpfI
  goto havenask_home_loop
)
for %%I in ("%HAVENASK_HOME%..") do set HAVENASK_HOME=%%~dpfI

rem now set the classpath
set HAVENASK_CLASSPATH=!HAVENASK_HOME!\lib\*

set HOSTNAME=%COMPUTERNAME%

if not defined HAVENASK_PATH_CONF (
  set HAVENASK_PATH_CONF=!HAVENASK_HOME!\config
)

rem now make HAVENASK_PATH_CONF absolute
for %%I in ("%HAVENASK_PATH_CONF%..") do set HAVENASK_PATH_CONF=%%~dpfI

set HAVENASK_DISTRIBUTION_TYPE=${havenask.distribution.type}
set HAVENASK_BUNDLED_JDK=${havenask.bundled_jdk}

if "%HAVENASK_BUNDLED_JDK%" == "false" (
  echo "warning: no-jdk distributions that do not bundle a JDK are deprecated and will be removed in a future release" >&2
)

cd /d "%HAVENASK_HOME%"

rem now set the path to java, pass "nojava" arg to skip setting JAVA_HOME and JAVA
if "%1" == "nojava" (
   exit /b
)

rem compariing to empty string makes this equivalent to bash -v check on env var
rem and allows to effectively force use of the bundled jdk when launching Havenask
rem by setting JAVA_HOME=
if "%JAVA_HOME%" == "" (
  set JAVA="%HAVENASK_HOME%\jdk\bin\java.exe"
  set JAVA_HOME="%HAVENASK_HOME%\jdk"
  set JAVA_TYPE=bundled jdk
) else (
  set JAVA="%JAVA_HOME%\bin\java.exe"
  set JAVA_TYPE=JAVA_HOME
)

if not exist !JAVA! (
  echo "could not find java in !JAVA_TYPE! at !JAVA!" >&2
  exit /b 1
)

rem do not let JAVA_TOOL_OPTIONS slip in (as the JVM does by default)
if defined JAVA_TOOL_OPTIONS (
  echo warning: ignoring JAVA_TOOL_OPTIONS=%JAVA_TOOL_OPTIONS%
  set JAVA_TOOL_OPTIONS=
)

rem JAVA_OPTS is not a built-in JVM mechanism but some people think it is so we
rem warn them that we are not observing the value of %JAVA_OPTS%
if defined JAVA_OPTS (
  (echo|set /p=warning: ignoring JAVA_OPTS=%JAVA_OPTS%; )
  echo pass JVM parameters via HAVENASK_JAVA_OPTS
)

rem check the Java version
%JAVA% -cp "%HAVENASK_CLASSPATH%" "org.havenask.tools.java_version_checker.JavaVersionChecker" || exit /b 1

