import sys
import io
import time
import os
import signal
from .util import (
    log_info,
    log_error,
    log_warning,
    log_debug,
    log_job_trace,
    shorten_job_id,
)
import select
import termios
import tty
import threading
from .util import console
import rich.status
from .parallel import async_raise
from collections import namedtuple

StatusReport = namedtuple(
    "StatusReport", ["running", "waiting", "done", "total", "failed"]
)


class ConsoleInteractive:
    def _set_terminal_raw(self):
        """Set almost all raw settings on the terminal, except for the output meddling
        - if we did that we get weird newlines from rich"""
        try:
            self.old_settings = termios.tcgetattr(sys.stdin)
            fd = sys.stdin.fileno()
            when = termios.TCSAFLUSH
            tty.setraw(fd)
            mode = termios.tcgetattr(sys.stdin.fileno())
            mode[1] = mode[1] | termios.OPOST  # termios.tcgetattr(fd)
            termios.tcsetattr(sys.stdin.fileno(), when, mode)
        except io.UnsupportedOperation:  # happens in tests that set it to console mode
            pass

    def _end_terminal_raw(self):
        if hasattr(self, "old_settings"):
            try:
                termios.tcsetattr(sys.stdin, termios.TCSADRAIN, self.old_settings)
            except io.UnsupportedOperation:  # see _set_terminal_raw
                pass

    def start(self, runner):
        self.runner = runner
        self.last_report_status_args = StatusReport(0, 0, 0, len(runner.jobs), 0)
        self.breaker = os.pipe()
        self.thread = threading.Thread(target=self.loop)
        self._set_terminal_raw()
        self.stopped = False
        self.leave_thread = False
        self.thread.start()
        log_info("Type 'help<enter>' to receive a list of valid commands")
        self._cmd = ""
        self.status = rich.status.Status("", console=console)
        self.status.start()
        self.report_status(self.last_report_status_args)

    def stop(self):
        """Called from the runner"""
        log_info("Leaving interactive mode")
        self.stopped = True
        if hasattr(self, "thread"):
            self.leave_thread = True
            #async_raise(self.thread.ident, KeyboardInterrupt)
            os.write(self.breaker[1], b"x")
            self._end_terminal_raw()
            log_job_trace("Terminating interactive thread")
            self.thread.join()
            log_job_trace("Terminated interactive thread")
            self.status.stop()
        del self.runner
        log_info("Left interactive mode")

    @property
    def cmd(self):
        return self._cmd

    @cmd.setter
    def cmd(self, value):
        self._cmd = value
        self.report_status(self.last_report_status_args)

    def loop(self):
        log_info("Entering interactive loop")
        while True:
            try:
                if self.leave_thread:
                    break
                try:
                    input = select.select([sys.stdin, self.breaker[0]], [], [], 10)[0]
                except io.UnsupportedOperation as e:
                    if "redirected stdin is pseudofile" in str(
                        e
                    ):  # running under pytest - no interactivity, but suport requesting it?
                        input = False
                    else:
                        raise
                if input:
                    if self.breaker[0] in input:
                        break
                    else:  # must have been stdin.
                        value = sys.stdin.read(1)
                        # log_info(f"received {repr(value)}")
                        if value == "\x03":  # ctrl-c:
                            self.cmd = ""
                        elif value == "\x1a":  # ctrl-z
                            os.kill(os.getpid(), signal.SIGTSTP)
                        elif ord("0") <= ord(value) <= ord("z") or value == " ":
                            self.cmd += value
                        elif value == "\x7f":  # backspace
                            self.cmd = self.cmd[:-1]
                        elif value == "\n" or value == "\r":
                            try:
                                if self.cmd:
                                    command = self.cmd
                                    args = ""
                                    if " " in command:
                                        command = command[: command.find(" ")]
                                        args = self.cmd[len(command) + 1 :].strip()
                                    self.cmd = ""
                                    if hasattr(self, "_cmd_" + command):
                                        getattr(self, "_cmd_" + command)(args)
                                    else:
                                        print("No such command")
                                else:
                                    self._cmd_default()
                                self.report_status(self.last_report_status_args)
                            except Exception as e:
                                log_error(
                                    f"An exception occured during command: {e} {type(e)}"
                                )
                                self.cmd = ""
                                continue

            except KeyboardInterrupt:
                break
        # log_job_trace("Leaving interactive loop")

    def report_status(self, report):
        self.last_report_status_args = report
        # msg = f"[dim]Running/Waiting Done/Total[/dim] {report.running} / {report.waiting} {report.done} / {report.total}."  # In flight: {len(self.runner.jobs_in_flight)} "
        msg = f"[dim]T:[/dim]{report.total} D:{report.done} R:{report.running} W:{report.waiting} F:{report.failed}"
        if self.cmd:
            msg += f" Cmd: {self.cmd}"
        else:
            if self.stopped:
                msg += " Exiting..."
            else:
                # msg += "Type help<enter> for commands"
                pass
        self.status.update(status=msg + "\r\n")

    def _cmd_help(self, _args):
        """print help"""
        print("Help for interactive mode")
        print("You have the following commands available")
        print("\t- <enter> - Show currently running jobs")
        for x in dir(self):
            if x.startswith("_cmd"):
                cmd = x[5:]
                if cmd:
                    print(f"\t {cmd} -  {getattr(self, x).__doc__}")
        # print("\t- help - this command")
        # print("\t- abort - kill current jobs and exit asap")
        # print("\t- stop - Wait for the currently running jobs to finish, then exit")
        # print("\t- reboot - After the pipegraph has ended, restart the current python script")
        # print("\t- restart - After the currently running jobs have ended, restart the current python script")

    def _cmd_default(self):
        """print the currently running jobs (mapped to enter)"""
        t = time.time()
        to_sort = []
        for job_id in self.runner.jobs_in_flight:
            try:
                rt = t - self.runner.jobs[job_id].start_time
                to_sort.append((rt, job_id))
            except KeyError:
                pass
        to_sort.sort()
        print(" | ".join(("Job_no", "Runtime", "Cores", "Job_id")))
        print(" | ".join(("------", "-------", "-----", "------")))
        for rt, job_id in to_sort:
            job = self.runner.jobs[job_id]
            job_no = job.job_number
            if job.waiting:
                rt = "waiting"
            else:
                rt = f"{rt:>6.2f}s"
            display_job_id = shorten_job_id(job_id)
            cores = job.actual_cores_needed if job.actual_cores_needed != -1 else "?"
            print(f"{job_no:>6} | {rt} | {cores} | {display_job_id}")
        print("")

    def _cmd_abort(self, _args):
        """Kill current jobs and exit (safely) asap"""
        log_info("Run aborted by command. Safely shutting down")
        self.runner.abort()
        self.stopped = True

    def _cmd_die(self, _args):
        """kill the current process without saving history"""
        log_error("Sic semper running processes")
        os.kill(os.getpid(), signal.SIGTERM)
        sys.exit(1)

    def _cmd_stop(self, _args):
        """Exit after current jobs finished"""
        if not self.stopped:
            log_info("Run stopped by command")
            waiting_for = []
            for job_id in self.runner.jobs_in_flight:
                try:
                    if not getattr(self.runner.jobs[job_id], "waiting", False):
                        waiting_for.append(job_id)
                except KeyError:
                    pass

            log_info(f"Having to wait for jobs: {sorted(waiting_for)}")
            self.runner.stop()
            self.stopped = True

    def _cmd_again(self, _args):
        """Restart the current python program after all jobs have completed"""
        log_info("Again command issued")
        self.runner.job_graph.restart_afterwards()

    def _cmd_stop_and_again(self, _args):
        "Stop after current jobs, then restart the current python program"
        # log_info("Stop_and_again command issued")
        # self.runner.stop()
        # self.stopped = True
        # self.runner.job_graph.restart_afterwards()
        self._cmd_stop(_args)
        self._cmd_again(_args)

    def _cmd_kill(self, args):
        """kill a running job (by id)"""
        try:
            job_no = int(args)
        except ValueError:
            print(f"Could not understand job number {repr(args)}- must be an integer")
            return
        for job in self.runner.jobs.values():
            if job.job_number == job_no:
                break
        else:
            print("Could not find job number")
            return
        if not job.job_id in self.runner.jobs_in_flight:
            print("Job not currently in flight - can't kill it")
            return
        if not job.resources.is_external():
            print("Job is not running in an external process - can't kill")
            return
        print("ok, killing job", job.job_id)
        log_info(f"Command kill {job.job_id} ")
        job.kill_if_running()
