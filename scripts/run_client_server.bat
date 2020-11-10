@echo off

set clientnum=1
::最多测试到500个client

set servernum=4

set batchsize=1
set clientprint=false
set verbose=false
set log=false

::获取父目录
pushd..
set home=%cd%
popd

set serverProcName=hotstuffserver.exe
set clientProcName=hotstuffclient.exe

set beg=1
set /a end=%servernum%

set tls=true

::启动服务端
for /l %%i in (%beg%,1,%end%) do (
start cmd /k "cd/d %home% && %serverProcName% --tls=%tls% --self-id %%i --privkey %home%/keys/r%%i.key --view-change 1"
)

:: --memprofile %home%/profileMem/mem%%i.prof

::start cmd /k "cd/d %home% && %serverProcName% --tls=%tls% --self-id %%i --privkey %home%/keys/r%%i.key --cpuprofile %home%/profileCPU/cpu%%i.prof"

::启动客户端
::start cmd /k "cd/d %home% && %clientProcName% --tls=%tls%"
