import time
import os
import psutil
while True:
    for p in psutil.process_iter():
        if 'sleep' in p.cmdline():
            print('pid', p.pid, p.cmdline(), 'sid', os.getpgid(p.pid))
    time.sleep(.1)
