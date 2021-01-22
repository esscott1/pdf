@echo off

rem Do whatever you want here over the files of this subdir, for example:

for %%f in (%1*.pdf) do (
echo copy %%f to S3
aws s3 cp %%f %2
sleep .5
)
exit /b