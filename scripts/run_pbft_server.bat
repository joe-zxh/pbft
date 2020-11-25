@echo off

if "%1"=="" (
    set servernum=4
    echo %servernum%
) else (    
    set servernum=%1%
    echo %servernum%
)

::获取父目录
pushd..
set home=%cd%
popd

set serverProcName=pbftserver.exe
set clientProcName=pbftclient.exe

set beg=1
set /a end=%servernum%

set tls=true

::启动服务端
for /l %%i in (%beg%,1,%end%) do (
start cmd /k "cd/d %home% && %serverProcName% --cluster-size %servernum% --tls=%tls% --self-id %%i --privkey %home%/keys/r%%i.key --batch-size 100"
)

:: --memprofile %home%/profileMem/mem%%i.prof

::start cmd /k "cd/d %home% && %serverProcName% --tls=%tls% --self-id %%i --privkey %home%/keys/r%%i.key --cpuprofile %home%/profileCPU/cpu%%i.prof"

::启动客户端
::start cmd /k "cd/d %home% && %clientProcName% --tls=%tls%"
