@echo off
setlocal
cd /d "%~dp0"
set "HADOOP_HOME=%USERPROFILE%\hadoop"
set "hadoop.home.dir=%HADOOP_HOME%"
python data_ingress\file_splitter.py data_ingress\demo_data.jsonl data_ingress\split_chunks 3
if errorlevel 1 exit /b 1
echo PySpark split completed.
