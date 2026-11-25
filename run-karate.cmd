@echo off
rem Double-click this file (or run it in a terminal) to check the prerequisites and run the Karate tests.
rem Extra options are passed on, for example:  run-karate.cmd -Feature kafka-consumer
rem -InstallMissing only offers to install a missing tool, and asks before each install.
rem -ExecutionPolicy Bypass applies to this one run only; it does not change your system settings.
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\run-karate.ps1" -InstallMissing %*
set EXITCODE=%ERRORLEVEL%
echo.
echo %cmdcmdline% | find /i "%~nx0" >nul && pause
exit /b %EXITCODE%
