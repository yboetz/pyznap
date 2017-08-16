import errno as _errno
import io
import logging
import re
import threading

from superprocess import Superprocess

log = logging.getLogger(__name__)

superprocess = Superprocess()

PIPE = superprocess.PIPE
STDOUT = superprocess.PIPE
STDERR = superprocess.STDERR
CalledProcessError = superprocess.CalledProcessError
check_call = superprocess.check_call
check_output = superprocess.check_output
popen = superprocess.popen

class ZFSError(OSError):
	def __init__(self, dataset):
		super(ZFSError, self).__init__(self.errno, self.strerror, dataset)

class DatasetNotFoundError(ZFSError):
	errno = _errno.ENOENT
	strerror = 'dataset does not exist'

class DatasetExistsError(ZFSError):
	errno = _errno.EEXIST
	strerror = 'dataset already exists'

class DatasetBusyError(ZFSError):
	errno = _errno.EBUSY
	strerror = 'dataset is busy'

class HoldTagNotFoundError(ZFSError):
	errno = _errno.ENOENT
	strerror = 'no such tag on this dataset'

class HoldTagExistsError(ZFSError):
	errno = _errno.EEXIST
	strerror = 'tag already exists on this dataset'

class CompletedProcess(superprocess.CompletedProcess):
	def check_returncode(self):
		# check for known errors of form "cannot <action> <dataset>: <reason>"
		if self.returncode == 1:
			pattern = r"^cannot ([^ ]+(?: [^ ]+)*?) ([^ :]+): (.+)$"
			match = re.search(pattern, self.stderr)
			if match:
				action, dataset, reason = match.groups()
				if dataset[0] == dataset[-1] == "'": dataset = dataset[1:-1]
				for Error in (DatasetNotFoundError,
						DatasetExistsError,
						DatasetBusyError,
						HoldTagNotFoundError,
						HoldTagExistsError,):
					if reason == Error.strerror:
						raise Error(dataset)

		# did not match known errors, defer to superclass
		super(CompletedProcess, self).check_returncode()

superprocess.CompletedProcess = CompletedProcess

class Popen(superprocess.Popen):
	def __init__(self, cmd, **kwargs):
		# zfs commands don't require setting both stdin and stdout
		stdin = kwargs.pop('stdin', None)
		stdout = kwargs.pop('stdout', None)
		if stdin is not None and stdout is not None:
			raise ValueError('only one of stdin or stdout may be set')

		# commands that accept input such as zfs receive may write
		# verbose output to stdout - redirect it to stderr
		if stdin is not None:
			stdout = superprocess.STDERR

		# use text mode by default
		universal_newlines = kwargs.pop('universal_newlines', True)

		# start process
		log.debug(' '.join(cmd))
		super(Popen, self).__init__(
			cmd, stdin=stdin, stdout=stdout, stderr=superprocess.PIPE,
			universal_newlines=universal_newlines, **kwargs)

		# set stderr aside for logging and ensure it is a text stream
		stderr, self.stderr = self.stderr, None
		if not isinstance(stderr, io.TextIOBase):
			if not isinstance(stderr, io.BufferedIOBase):
				stderr = io.BufferedReader(stderr)
			stderr = io.TextIOWrapper(stderr)

		# write stderr to log and store most recent line for analysis
		_stderr = [None]
		def log_stderr():
			with stderr as f:
				for line in f:
					msg = line.rstrip('\n')
					log.debug(msg)
					_stderr[0] = msg
		t = threading.Thread(target=log_stderr)
		t.daemon = True
		t.start()
		self._stderr_read = lambda: t.join() or _stderr[0]

	def communicate(self, *args, **kwargs):
		stdout, _ = super(Popen, self).communicate(*args, **kwargs)
		output = None if stdout is None else \
			[tuple(line.split('\t')) for line in stdout.splitlines()]
		return output, self._stderr_read()

superprocess.Popen = Popen
