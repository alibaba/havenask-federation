@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set NOJAVA=nojava
if /i "%1" == "install" set NOJAVA=

call "%~dp0havenask-env.bat" %NOJAVA% || exit /b 1

set EXECUTABLE=%HAVENASK_HOME%\bin\havenask-service-x64.exe
if "%SERVICE_ID%" == "" set SERVICE_ID=havenask-service-x64
set ARCH=64-bit

if EXIST "%EXECUTABLE%" goto okExe
echo havenask-service-x64.exe was not found...
exit /B 1

:okExe
set HAVENASK_VERSION=${project.version}

if "%SERVICE_LOG_DIR%" == "" set SERVICE_LOG_DIR=%HAVENASK_HOME%\logs

if "x%1x" == "xx" goto displayUsage
set SERVICE_CMD=%1
shift
if "x%1x" == "xx" goto checkServiceCmd
set SERVICE_ID=%1

:checkServiceCmd

if "%LOG_OPTS%" == "" set LOG_OPTS=--LogPath "%SERVICE_LOG_DIR%" --LogPrefix "%SERVICE_ID%" --StdError auto --StdOutput auto

if /i %SERVICE_CMD% == install goto doInstall
if /i %SERVICE_CMD% == remove goto doRemove
if /i %SERVICE_CMD% == start goto doStart
if /i %SERVICE_CMD% == stop goto doStop
if /i %SERVICE_CMD% == manager goto doManagment
echo Unknown option "%SERVICE_CMD%"
exit /B 1

:displayUsage
echo.
echo Usage: havenask-service.bat install^|remove^|start^|stop^|manager [SERVICE_ID]
goto:eof

:doStart
"%EXECUTABLE%" //HAVENASK//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto started
echo Failed starting '%SERVICE_ID%' service
exit /B 1
goto:eof
:started
echo The service '%SERVICE_ID%' has been started
goto:eof

:doStop
"%EXECUTABLE%" //SS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto stopped
echo Failed stopping '%SERVICE_ID%' service
exit /B 1
goto:eof
:stopped
echo The service '%SERVICE_ID%' has been stopped
goto:eof

:doManagment
set EXECUTABLE_MGR=%HAVENASK_HOME%\bin\havenask-service-mgr
"%EXECUTABLE_MGR%" //HAVENASK//%SERVICE_ID%
if not errorlevel 1 goto managed
echo Failed starting service manager for '%SERVICE_ID%'
exit /B 1
goto:eof
:managed
echo Successfully started service manager for '%SERVICE_ID%'.
goto:eof

:doRemove
rem Remove the service
"%EXECUTABLE%" //DS//%SERVICE_ID% %LOG_OPTS%
if not errorlevel 1 goto removed
echo Failed removing '%SERVICE_ID%' service
exit /B 1
goto:eof
:removed
echo The service '%SERVICE_ID%' has been removed
goto:eof

:doInstall
echo Installing service      :  "%SERVICE_ID%"
echo Using JAVA_HOME (%ARCH%):  "%JAVA_HOME%"

rem Check JVM server dll first
if exist "%JAVA_HOME%\jre\bin\server\jvm.dll" (
	set JVM_DLL=\jre\bin\server\jvm.dll
	goto foundJVM
)

rem Check 'server' JRE (JRE installed on Windows Server)
if exist "%JAVA_HOME%\bin\server\jvm.dll" (
	set JVM_DLL=\bin\server\jvm.dll
	goto foundJVM
) else (
  	echo JAVA_HOME ("%JAVA_HOME%"^) points to an invalid Java installation (no jvm.dll found in "%JAVA_HOME%\jre\bin\server" or "%JAVA_HOME%\bin\server"^). Exiting...
  	goto:eof
)

:foundJVM
if not defined HAVENASK_TMPDIR (
  for /f "tokens=* usebackq" %%a in (`CALL %JAVA% -cp "!HAVENASK_CLASSPATH!" "org.havenask.tools.launchers.TempDirectory"`) do set HAVENASK_TMPDIR=%%a
)

rem The JVM options parser produces the final JVM options to start
rem Havenask. It does this by incorporating JVM options in the following
rem way:
rem   - first, system JVM options are applied (these are hardcoded options in
rem     the parser)
rem   - second, JVM options are read from jvm.options and
rem     jvm.options.d/*.options
rem   - third, JVM options from HAVENASK_JAVA_OPTS are applied
rem   - fourth, ergonomic JVM options are applied

@setlocal
for /F "usebackq delims=" %%a in (`CALL %JAVA% -cp "!HAVENASK_CLASSPATH!" "org.havenask.tools.launchers.JvmOptionsParser" "!HAVENASK_PATH_CONF!" ^|^| echo jvm_options_parser_failed`) do set HAVENASK_JAVA_OPTS=%%a
@endlocal & set "MAYBE_JVM_OPTIONS_PARSER_FAILED=%HAVENASK_JAVA_OPTS%" & set HAVENASK_JAVA_OPTS=%HAVENASK_JAVA_OPTS%

if "%MAYBE_JVM_OPTIONS_PARSER_FAILED%" == "jvm_options_parser_failed" (
  exit /b 1
)

rem The output of the JVM options parses is space-delimited. We need to
rem convert to semicolon-delimited and avoid doubled semicolons.
@setlocal
if not "%HAVENASK_JAVA_OPTS%" == "" (
  set HAVENASK_JAVA_OPTS=!HAVENASK_JAVA_OPTS: =;!
  set HAVENASK_JAVA_OPTS=!HAVENASK_JAVA_OPTS:;;=;!
)
@endlocal & set HAVENASK_JAVA_OPTS=%HAVENASK_JAVA_OPTS%

if "%HAVENASK_JAVA_OPTS:~-1%"==";" set HAVENASK_JAVA_OPTS=%HAVENASK_JAVA_OPTS:~0,-1%

echo %HAVENASK_JAVA_OPTS%

@setlocal EnableDelayedExpansion
for %%a in ("%HAVENASK_JAVA_OPTS:;=","%") do (
  set var=%%a
  set other_opt=true
  if "!var:~1,4!" == "-Xms" (
    set XMS=!var:~5,-1!
    set other_opt=false
    call:convertxm !XMS! JVM_MS
  )
  if "!var:~1,16!" == "-XX:MinHeapSize=" (
    set XMS=!var:~17,-1!
    set other_opt=false
    call:convertxm !XMS! JVM_MS
  )
  if "!var:~1,4!" == "-Xmx" (
    set XMX=!var:~5,-1!
    set other_opt=false
    call:convertxm !XMX! JVM_MX
  )
  if "!var:~1,16!" == "-XX:MaxHeapSize=" (
    set XMX=!var:~17,-1!
    set other_opt=false
    call:convertxm !XMX! JVM_MX
  )
  if "!var:~1,4!" == "-Xss" (
    set XSS=!var:~5,-1!
    set other_opt=false
    call:convertxk !XSS! JVM_SS
  )
  if "!var:~1,20!" == "-XX:ThreadStackSize=" (
    set XSS=!var:~21,-1!
    set other_opt=false
    call:convertxk !XSS! JVM_SS
  )
  if "!other_opt!" == "true" set OTHER_JAVA_OPTS=!OTHER_JAVA_OPTS!;!var!
)
@endlocal & set JVM_MS=%JVM_MS% & set JVM_MX=%JVM_MX% & set JVM_SS=%JVM_SS% & set OTHER_JAVA_OPTS=%OTHER_JAVA_OPTS%

if "%JVM_MS%" == "" (
  echo minimum heap size not set; configure using -Xms via "%HAVENASK_PATH_CONF%/jvm.options.d", or HAVENASK_JAVA_OPTS
  goto:eof
)
if "%JVM_MX%" == "" (
  echo maximum heap size not set; configure using -Xmx via "%HAVENASK_PATH_CONF%/jvm.options.d", or HAVENASK_JAVA_OPTS
  goto:eof
)
if "%JVM_SS%" == "" (
  echo thread stack size not set; configure using -Xss via "%HAVENASK_PATH_CONF%/jvm.options.d", or HAVENASK_JAVA_OPTS
  goto:eof
)
set OTHER_JAVA_OPTS=%OTHER_JAVA_OPTS:"=%
set OTHER_JAVA_OPTS=%OTHER_JAVA_OPTS:~1%

set HAVENASK_PARAMS=-Dhavenask;-Dhavenask.path.home="%HAVENASK_HOME%";-Dhavenask.path.conf="%HAVENASK_PATH_CONF%";-Dhavenask.distribution.type="%HAVENASK_DISTRIBUTION_TYPE%";-Dhavenask.bundled_jdk="%HAVENASK_BUNDLED_JDK%"

if "%HAVENASK_START_TYPE%" == "" set HAVENASK_START_TYPE=manual
if "%HAVENASK_STOP_TIMEOUT%" == "" set HAVENASK_STOP_TIMEOUT=0

if "%SERVICE_DISPLAY_NAME%" == "" set SERVICE_DISPLAY_NAME=Havenask %HAVENASK_VERSION% (%SERVICE_ID%)
if "%SERVICE_DESCRIPTION%" == "" set SERVICE_DESCRIPTION=Havenask %HAVENASK_VERSION% Windows Service - https://havenask.org

if not "%SERVICE_USERNAME%" == "" (
	if not "%SERVICE_PASSWORD%" == "" (
		set SERVICE_PARAMS=%SERVICE_PARAMS% --ServiceUser "%SERVICE_USERNAME%" --ServicePassword "%SERVICE_PASSWORD%"
	)
)
"%EXECUTABLE%" //IS//%SERVICE_ID% --Startup %HAVENASK_START_TYPE% --StopTimeout %HAVENASK_STOP_TIMEOUT% --StartClass org.havenask.bootstrap.Havenask --StartMethod main ++StartParams --quiet --StopClass org.havenask.bootstrap.Havenask --StopMethod close --Classpath "%HAVENASK_CLASSPATH%" --JvmMs %JVM_MS% --JvmMx %JVM_MX% --JvmSs %JVM_SS% --JvmOptions %OTHER_JAVA_OPTS% ++JvmOptions %HAVENASK_PARAMS% %LOG_OPTS% --PidFile "%SERVICE_ID%.pid" --DisplayName "%SERVICE_DISPLAY_NAME%" --Description "%SERVICE_DESCRIPTION%" --Jvm "%JAVA_HOME%%JVM_DLL%" --StartMode jvm --StopMode jvm --StartPath "%HAVENASK_HOME%" %SERVICE_PARAMS% ++Environment HOSTNAME="%%COMPUTERNAME%%"

if not errorlevel 1 goto installed
echo Failed installing '%SERVICE_ID%' service
exit /B 1
goto:eof

:installed
echo The service '%SERVICE_ID%' has been installed.
goto:eof

:err
echo JAVA_HOME environment variable must be set!
pause
goto:eof

rem ---
rem Function for converting Xm[s|x] values into MB which Commons Daemon accepts
rem ---
:convertxm
set value=%~1
rem extract last char (unit)
set unit=%value:~-1%
rem assume the unit is specified
set conv=%value:~0,-1%

if "%unit%" == "k" goto kilo
if "%unit%" == "K" goto kilo
if "%unit%" == "m" goto mega
if "%unit%" == "M" goto mega
if "%unit%" == "g" goto giga
if "%unit%" == "G" goto giga

rem no unit found, must be bytes; consider the whole value
set conv=%value%
rem convert to KB
set /a conv=%conv% / 1024
:kilo
rem convert to MB
set /a conv=%conv% / 1024
goto mega
:giga
rem convert to MB
set /a conv=%conv% * 1024
:mega
set "%~2=%conv%"
goto:eof

:convertxk
set value=%~1
rem extract last char (unit)
set unit=%value:~-1%
rem assume the unit is specified
set conv=%value:~0,-1%

if "%unit%" == "k" goto kilo
if "%unit%" == "K" goto kilo
if "%unit%" == "m" goto mega
if "%unit%" == "M" goto mega
if "%unit%" == "g" goto giga
if "%unit%" == "G" goto giga

rem no unit found, must be bytes; consider the whole value
set conv=%value%
rem convert to KB
set /a conv=%conv% / 1024
goto kilo
:mega
rem convert to KB
set /a conv=%conv% * 1024
goto kilo
:giga
rem convert to KB
set /a conv=%conv% * 1024 * 1024
:kilo
set "%~2=%conv%"
goto:eof

endlocal
endlocal

exit /b %ERRORLEVEL%
