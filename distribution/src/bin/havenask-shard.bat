@echo off

setlocal enabledelayedexpansion
setlocal enableextensions

set HAVENASK_MAIN_CLASS=org.havenask.index.shard.ShardToolCli
call "%~dp0havenask-cli.bat" ^
  %%* ^
  || goto exit

endlocal
endlocal
:exit
exit /b %ERRORLEVEL%
