@echo off 
if exist ..\bin\Debug\Microsoft.Research.Peloponnese.Wrapper.cmd del ..\bin\Debug\Microsoft.Research.Peloponnese.Wrapper.cmd
if exist ..\bin\Release\Microsoft.Research.Peloponnese.Wrapper.cmd del ..\bin\Release\Microsoft.Research.Peloponnese.Wrapper.cmd

@rem copy cmd file to bin dirs
        copy Microsoft.Research.Peloponnese.Wrapper.cmd ..\bin\Debug\Microsoft.Research.Peloponnese.Wrapper.cmd
        copy Microsoft.Research.Peloponnese.Wrapper.cmd ..\bin\Release\Microsoft.Research.Peloponnese.Wrapper.cmd
        exit /b

:failed
    echo Java Compilation failed. skipping jar creation

