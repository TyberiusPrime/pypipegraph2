import pypipegraph2 as ppg
import sys
import subprocess
from pathlib import Path


try:
    Path('deleteme').unlink()
except:
    pass
if Path('.ppg').exists():
    import shutil
    shutil.rmtree('.ppg')

ppg.new()
def doit(output_filename):
    subprocess.check_call(['./rust_stdout_stderr'])
    print("pythonstdout\n")
    sys.stderr.write("pythonstderr\n")
    output_filename.write_text('done')


j = ppg.TempFileGeneratingJob('.ppg/deleteme', doit)
j2 = ppg.FileGeneratingJob('.ppg/shu', lambda of : of.write_text('hello'))
j2.depends_on(j)
j2()

Path('.ppg/shu').unlink()
Path('.ppg/deleteme').write_text('done')
j2()

