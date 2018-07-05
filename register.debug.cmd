@echo Registering RTD Server
%SystemRoot%\Microsoft.NET\Framework\v4.0.30319\RegAsm.exe %cd%\kafka-rtd\bin\Debug\kafka-rtd.dll /codebase

@if errorlevel 1 pause
