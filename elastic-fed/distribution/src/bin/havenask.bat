@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

SET params='%*'
SET checkpassword=Y

:loop
FOR /F "usebackq tokens=1* delims= " %%A IN (!params!) DO (
    SET current=%%A
    SET params='%%B'
	SET silent=N

	IF "!current!" == "-s" (
		SET silent=Y
	)
	IF "!current!" == "--silent" (
		SET silent=Y
	)

	IF "!current!" == "-h" (
		SET checkpassword=N
	)
	IF "!current!" == "--help" (
		SET checkpassword=N
	)

	IF "!current!" == "-V" (
		SET checkpassword=N
	)
	IF "!current!" == "--version" (
		SET checkpassword=N
	)

	IF "!silent!" == "Y" (
		SET nopauseonerror=Y
	) ELSE (
	    IF "x!newparams!" NEQ "x" (
	        SET newparams=!newparams! !current!
        ) ELSE (
            SET newparams=!current!
        )
	)

    IF "x!params!" NEQ "x" (
		GOTO loop
	)
)

CALL "%~dp0havenask-env.bat" || exit /b 1
IF ERRORLEVEL 1 (
	IF NOT DEFINED nopauseonerror (
		PAUSE
	)
	EXIT /B %ERRORLEVEL%
)

SET KEYSTORE_PASSWORD=
IF "%checkpassword%"=="Y" (
  CALL "%~dp0havenask-keystore.bat" has-passwd --silent
  IF !ERRORLEVEL! EQU 0 (
    SET /P KEYSTORE_PASSWORD=Havenask keystore password:
    IF !ERRORLEVEL! NEQ 0 (
      ECHO Failed to read keystore password on standard input
      EXIT /B !ERRORLEVEL!
    )
  )
)

if not defined HAVENASK_TMPDIR (
  for /f "tokens=* usebackq" %%a in (`CALL %JAVA% -cp "!HAVENASK_CLASSPATH!" "org.havenask.tools.launchers.TempDirectory"`) do set  HAVENASK_TMPDIR=%%a
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

rem windows batch pipe will choke on special characters in strings
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^^=^^^^!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^&=^^^&!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^|=^^^|!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^<=^^^<!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^>=^^^>!
SET KEYSTORE_PASSWORD=!KEYSTORE_PASSWORD:^\=^^^\!

ECHO.!KEYSTORE_PASSWORD!| %JAVA% %HAVENASK_JAVA_OPTS% -Dhavenask ^
  -Dhavenask.path.home="%HAVENASK_HOME%" -Dhavenask.path.conf="%HAVENASK_PATH_CONF%" ^
  -Dhavenask.distribution.type="%HAVENASK_DISTRIBUTION_TYPE%" ^
  -Dhavenask.bundled_jdk="%HAVENASK_BUNDLED_JDK%" ^
  -cp "%HAVENASK_CLASSPATH%" "org.havenask.bootstrap.Havenask" !newparams!

endlocal
endlocal
exit /b %ERRORLEVEL%
