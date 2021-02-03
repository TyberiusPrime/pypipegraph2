import sys
import time
import os
import signal
from loguru import logger
import select
import termios
import tty
import threading
from .parallel import async_raise
from .util import console
import rich.status


class ConsoleInteractive:

    def _set_terminal_raw(self):
        """Set almost all raw settings on the terminal, except for the output meddling
        - if we did that we get weird newlines from rich"""
        self.old_settings = termios.tcgetattr(sys.stdin)
        fd = sys.stdin.fileno()
        when = termios.TCSAFLUSH
        tty.setraw(fd)
        mode = termios.tcgetattr(sys.stdin.fileno())
        mode[1] = mode[1] | termios.OPOST #termios.tcgetattr(fd)
        termios.tcsetattr(sys.stdin.fileno(), when, mode)

    def _end_terminal_raw(self):
        termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self.old_settings)


    def start(self, runner):
        self.runner = runner
        self.last_report_status_args = (0,0,0)
        self._set_terminal_raw()
        self.thread = threading.Thread(target=self.loop)
        self.stopped = False
        self.thread.start()
        print("Type 'help<enter>' to receive a list of valid commands")
        self._cmd = ""
        self.status = rich.status.Status('',console=console)
        self.status.start()

    def stop(self):
        self.stopped = True
        self._end_terminal_raw()
        logger.info("Terminating interactive thread")
        self.thread.join()
        logger.info("Terminated interactive thread")
        self.status.stop()
        del self.runner
        # async_raise(self.thread.ident, SystemExit)

    @property 
    def cmd(self):
        return self._cmd

    @cmd.setter
    def cmd(self, value):
        self._cmd = value
        self.report_status(*self.last_report_status_args)

    def loop(self):
        logger.info("Entering interactive loop")
        while True:
            try:
                if self.stopped:
                    break
                input = bool(select.select([sys.stdin], [], [], 1)[0])
                if input:
                    value = sys.stdin.read(1)
                    #logger.info(f"received {repr(value)}")
                    if value == "\x03":  # ctrl-c:
                        self.cmd = ""
                    elif value == "\x1a":  # ctrl-z
                        os.kill(os.getpid(), signal.SIGTSTP)
                    elif ord("0") <= ord(value) <= ord("z"):
                        self.cmd += value
                    elif value == '\x7f': # backspace
                        self.cmd = self.cmd[:-1]
                    elif value == "\n" or value == '\r':
                        try:
                            if self.cmd:
                                command = self.cmd
                                args = ''
                                if ' ' in command:
                                    command = command[:command.find(" ")]
                                    args =command[command.find(" ")+1:]
                                self.cmd = ""
                                if hasattr(self, '_cmd_' + command):
                                    getattr(self, '_cmd_' + command)(args)
                                else:
                                    print("No such command")
                            else:
                                self._cmd_default()
                        except Exception as e:
                            logger.error(e)
                            self.cmd = ''
                            continue

            except KeyboardInterrupt:
                break
        logger.info("Leaving interactive loop")

    def report_status(self, jobs_done, jobs_failed, jobs_total):
        self.last_report_status_args = jobs_done, jobs_failed, jobs_total
        msg = f"[red]Progress[/red] {jobs_done} / {jobs_total}. "
        if self.cmd:
            msg += f"Cmd: {self.cmd}"
        else:
            msg += "Type help<enter> for commands"
        self.status.update(status=msg)


    def _cmd_help(self, _args):
        print("Help for interactive mode")
        print("You have the following commands available")
        print("\t- <enter> - Show currently running jobs")
        for x in dir(self):
            if x.startswith('_cmd'):
                cmd = x[5:]
                if cmd:
                    print(f"\t {cmd} -  {getattr(self, x).__doc__}")
        #print("\t- help - this command")
        #print("\t- abort - kill current jobs and exit asap")
        #print("\t- stop - Wait for the currently running jobs to finish, then exit")
        #print("\t- reboot - After the pipegraph has ended, restart the current python script")
        #print("\t- restart - After the currently running jobs have ended, restart the current python script")

    def _cmd_default(self):
        t = time.time()
        to_sort = []
        for job_id in self.runner.jobs_in_flight:
            rt = t - self.runner.jobs[job_id].start_time
            to_sort.append((rt,  job_id))
        to_sort.sort()
        for rt, job_id in to_sort:
            job_no = self.runner.jobs[job_id].job_number
            print(f"{job_no} {job_id}: Running for {rt:.2f} seconds")
        print("")

    def _cmd_abort(self, _args):
        """Kill current jobs and exit (safely) asap"""
        logger.info("Run aborted")
        self.runner.abort()
        self.stopped = True

