@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set HAVENASK_MAIN_CLASS=org.havenask.plugins.PluginCli
set HAVENASK_ADDITIONAL_CLASSPATH_DIRECTORIES=lib/tools/plugin-cli
call "%~dp0havenask-cli.bat" ^
  %%* ^
  || goto exit


endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
