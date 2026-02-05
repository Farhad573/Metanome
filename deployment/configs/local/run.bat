REM new port to avoid conflict with old version running on same system, to make comparison possible.
@start java -Xmx2g -jar webapp-runner.jar backend --port 5172
@start java -Xmx2g -jar webapp-runner.jar frontend