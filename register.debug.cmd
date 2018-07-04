@echo off

echo Registering RTD Server
%SystemRoot%\Microsoft.NET\Framework\v4.0.30319\RegAsm.exe .\Redis-rtd\bin\Debug\kafka-rtd.dll /codebase

if errorlevel 1 pause
