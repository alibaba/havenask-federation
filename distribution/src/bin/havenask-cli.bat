call "%~dp0havenask-env.bat" || exit /b 1

if defined HAVENASK_ADDITIONAL_SOURCES (
  for %%a in ("%HAVENASK_ADDITIONAL_SOURCES:;=","%") do (
    call "%~dp0%%a"
  )
)

if defined HAVENASK_ADDITIONAL_CLASSPATH_DIRECTORIES (
  for %%a in ("%HAVENASK_ADDITIONAL_CLASSPATH_DIRECTORIES:;=","%") do (
    set HAVENASK_CLASSPATH=!HAVENASK_CLASSPATH!;!HAVENASK_HOME!/%%a/*
  )
)

rem use a small heap size for the CLI tools, and thus the serial collector to
rem avoid stealing many CPU cycles; a user can override by setting HAVENASK_JAVA_OPTS
set HAVENASK_JAVA_OPTS=-Xms4m -Xmx64m -XX:+UseSerialGC %HAVENASK_JAVA_OPTS%

%JAVA% ^
  %HAVENASK_JAVA_OPTS% ^
  -Dhavenask.path.home="%HAVENASK_HOME%" ^
  -Dhavenask.path.conf="%HAVENASK_PATH_CONF%" ^
  -Dhavenask.distribution.type="%HAVENASK_DISTRIBUTION_TYPE%" ^
  -cp "%HAVENASK_CLASSPATH%" ^
  "%HAVENASK_MAIN_CLASS%" ^
  %*

exit /b %ERRORLEVEL%
