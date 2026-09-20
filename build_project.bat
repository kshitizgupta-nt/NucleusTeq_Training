@echo off
setlocal
cd /d "%~dp0lumi"
call mvnw.cmd clean package
if errorlevel 1 (echo Maven build failed.& exit /b 1)
echo Build completed successfully.
