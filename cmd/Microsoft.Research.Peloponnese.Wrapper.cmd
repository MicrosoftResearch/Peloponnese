@echo off
setlocal
for /f "tokens=1 delims=, " %%d in ("%LOG_DIRS%") do set _logdir=%%d
set _stdout=%1
shift
set _stderr=%1
shift
set params=%1

:loop
shift
if [%1]==[] goto afterloop
set params=%params% %1
goto loop

:afterloop
call %params% 1>%_logdir%\%_stdout% 2>%_logdir%\%_stderr%

rem if defined PELOPONNESE_COPY_YARN_LOGS (
rem   %hadoop_yarn_home%\bin\hdfs dfs -mkdir -p %PELOPONNESE_COPY_YARN_LOGS%/%CONTAINER_ID%
rem   cd %LOG_DIRS%
rem   %hadoop_yarn_home%\bin\hdfs dfs -put * %PELOPONNESE_COPY_YARN_LOGS%/%CONTAINER_ID%
rem )
