@echo off

if exist ..\bin\Debug\Microsoft.Research.Peloponnese.HadoopBridge.jar del ..\bin\Debug\Microsoft.Research.Peloponnese.HadoopBridge.jar
if exist ..\bin\Release\Microsoft.Research.Peloponnese.HadoopBridge.jar del ..\bin\Release\Microsoft.Research.Peloponnese.HadoopBridge.jar
if exist ..\bin\Debug\Microsoft.Research.Peloponnese.YarnLauncher.jar del ..\bin\Debug\Microsoft.Research.Peloponnese.YarnLauncher.jar
if exist ..\bin\Release\Microsoft.Research.Peloponnese.YarnLauncher.jar del ..\bin\Release\Microsoft.Research.Peloponnese.YarnLauncher.jar

@rem compile java classes
javac -d . com\microsoft\research\peloponnese\AppMaster.java com\microsoft\research\peloponnese\HdfsBridge.java 
javac -d . YarnApp.java
@rem javac -d . PeloponneseResourceLocalizer.java

@rem package java classes into jar
if errorlevel 1 goto failed
        jar -cvf ..\bin\Debug\Microsoft.Research.Peloponnese.HadoopBridge.jar com
        jar -cvf ..\bin\Release\Microsoft.Research.Peloponnese.HadoopBridge.jar com
        jar -cvfe ..\bin\Debug\Microsoft.Research.Peloponnese.YarnLauncher.jar YarnApp YarnApp.class
        jar -cvfe ..\bin\Release\Microsoft.Research.Peloponnese.YarnLauncher.jar YarnApp YarnApp.class
@rem         jar -cvfe ..\bin\Debug\Microsoft.Research.Peloponnese.ResourceLocalizer.jar PeloponneseResourceLocalizer PeloponneseResourceLocalizer.class
        exit /b

:failed
    echo Java Compilation failed. skipping jar creation

