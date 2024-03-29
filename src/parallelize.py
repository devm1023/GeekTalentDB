from cloudpickle import dump, load
import sys
import os
import errno
import uuid
import time
import subprocess
import numpy
import itertools
from datetime import datetime


def _silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

class BasicRunner:
    """Base class for runners (classes that control individual parallel jobs).

    Note:
      Derived classes must implement the :py:meth:`~my_fitter.BasicRunner.poll`
      and :py:meth:`~my_fitter.BasicRunner.cancel` methods and initialise the
      `pid` attribute.

    Args:
      name (str): The name of the group of parallel jobs.
      jobid (int): The number of the parallel job.
      workdir (str): The working directory where the jobs are executed and
       where the temporary files are kept. The path may be relative.
      script (str): The name of the python script to run. Includes the path,
        which may be relative.
      args (list of str): Command line arguments to pass to the Python script.
      stdout (str): Name of the file to which STDOUT output of the script should
        be re-directed. Includes the path, which may be relative.
      stderr (str): Name of the file to which STDERR output of the script should
        be re-directed. Includes the path, which may be relative.
      pyexec (str, optional): Name and path to the Python executable to use.
        Defaults to ``sys.executable``.
      options (str or list of str, optional): Extra command line options for the
        batch system. Defaults to ``[]``.
      log (output stream or None, optional): Output stream for log messages.
        Defaults to ``None``, in which case no messages are printed.
      loglevel (int, optional): Amount of information to log. Defaults to
        1, in which case only job statuses are logged.

    Attributes:
      running (bool): ``True`` if the job is currently running.
      finished (bool): ``True`` if the job has finished (normally or abnormally).
      name (str): Same as keyword argument `name`.
      jobid (int): Same as keyword argument `jobid`.
      workdir (str): Same as keyword argument `workdir`.
      script (str): Same as keyword argument `script`.
      args (list of str): Same as keyword argument `args`.
      stdout (str): Same as keyword argument `stdout`.
      stderr (str): Same as keyword argument `stderr`.
      pyexec (str): Same as keyword argument `pyexec`.
      options (str or list of str): Same as keyword argument `options`.
      pid (int): The process ID of the job on the local system or batch queue.

    """
    def __init__(self, name, jobid, workdir, script, args, stdout, stderr,
                 pyexec=sys.executable, options=[], log=None, loglevel=1):
        self.name = name
        self.jobid = jobid
        self.workdir = workdir
        self.script = script
        self.args = list(args)
        self.stdout = stdout
        self.stderr = stderr
        self.pyexec = pyexec
        self.running = False
        self.finished = False
        self.options = options
        if isinstance(self.options, str):
            self.options = self.options.split()
        self.log = log
        self.loglevel = loglevel
        self.pid = None

    def cancel(self):
        """Cancel the job.

        Note:
          Derived classes must implement this. The job should be cancelled,
          ``self.running`` should be set to ``False`` and ``self.finished``
          to ``True``.

        """
        raise NotImplementedError()

    def poll(self):
        """Update the job status.

        Note:
          Derived classes must implement this. If the job is pending
          (not started yet) it should set ``self.running`` and ``self.finished``
          to ``False``. If the job is currently running it should set
          ``self.running`` to ``True`` and ``self.finished`` to ``False``.
          If the job has finished (normally or abnormally) it should set
          ``self.running`` to ``False`` and ``self.finished`` to ``True``.

        """
        raise NotImplementedError()


class LocalRunner(BasicRunner):
    """Run job as independent process on local machine.

    """
    def __init__(self, *args, **kwargs):
        BasicRunner.__init__(self, *args, **kwargs)
        self.outfile = open(os.path.join(self.workdir, self.stdout), 'a')
        self.errfile = open(os.path.join(self.workdir, self.stderr), 'a')
        self.outfile.write(' '.join([self.pyexec, self.script]+self.args)+'\n')
        self.outfile.flush()
        self._proc = subprocess.Popen([self.pyexec, self.script]+self.args,
                                      stdout=self.outfile, stderr=self.errfile,
                                      cwd=self.workdir)
        self.running = True
        self.pid = self._proc.pid

    def cancel(self):
        self.poll()
        if not self.finished:
            self._proc.kill()
        self.outfile.close()
        self.errfile.close()
        self.finished = True
        self.running = False

    def poll(self):
        if self.finished:
            return
        self.running = self._proc.poll() is None
        self.finished = not self.running
        if self.finished:
            self.outfile.close()
            self.errfile.close()


class SlurmRunner(BasicRunner):
    """Submit job to the SLURM batch system.

    """
    def __init__(self, *args, **kwargs):
        BasicRunner.__init__(self, *args, **kwargs)
        self.workdir = os.path.abspath(self.workdir)
        self.script = os.path.abspath(self.script)
        self.args = map(os.path.abspath, self.args)
        self.stdout = os.path.abspath(self.stdout)
        self.stderr = os.path.abspath(self.stderr)
        self.pyexec = os.path.abspath(self.pyexec)
        self._runscript = \
            os.path.join(self.workdir, self.name+'.'+str(self.jobid)+'.sh')
        fout = open(self._runscript, 'w')
        fout.write('#!/bin/bash\n')
        cmd = ' '.join([self.pyexec, self.script]+self.args)
        fout.write('echo '+cmd+'\n')
        fout.write('srun '+cmd+'\n')
        fout.close()

        cmd = ['sbatch', '-D', self.workdir, '-o', self.stdout,
               '-e', self.stderr, '--job-name='+self.name, '--ntasks=1'] + \
               self.options + [self._runscript]
        if self.log is not None and self.loglevel >= 2:
            self.log.write(' '.join(cmd)+'\n')
            self.log.flush()
        output = subprocess.check_output(cmd)
        if self.log is not None and self.loglevel >= 2:
            self.log.write(output+'\n')
            self.log.flush()

        self.pid = int(output.split()[-1])

    def cancel(self):
        self.poll()
        if not self.finished:
            subprocess.call(['scancel', str(self.pid)])
            self.running = False
            self.finished = True
            os.remove(self._runscript)

    def poll(self):
        if self.finished:
            return
        try:
            cmd = ['squeue', '-j', str(self.pid), '-h', '--format', '%i %t']
            if self.log is not None and self.loglevel >= 3:
                self.log.write('Checking status:\n')
                self.log.write(' '.join(cmd)+'\n')
                self.log.flush()
            output = subprocess.check_output(cmd)
            if self.log is not None and self.loglevel >= 3:
                self.log.write(output+'\n')
                self.log.flush()
            output = output.split()
            if len(output) != 2:
                raise ValueError('Cannot parse output')
            id = int(output[0])
        except (ValueError, subprocess.CalledProcessError):
            _silentremove(os.path.join(self.workdir,
                                       self.name+'.'+str(self.jobid)+'.sh'))
            self.running = False
            self.finished = True
            return

        self.running = (id == self.pid and output[1] == 'R')
        self.finished = False


def _parallelize(f, batches, workdir='.', prefix=None, prelude=None,
                 runner=LocalRunner, tries=1, log=sys.stdout, loglevel=1,
                 refresh=1, cleanup=0, append=False, autocancel=True,
                 options=[], timeout=None):
    if os.path.exists(workdir):
        if not os.path.isdir(workdir):
            raise RuntimeError('Cannot create `'+workdir+'`. File exists.')
    else:
        os.makedirs(workdir)

    if prefix is None:
        prefix = str(uuid.uuid4())
    stem = os.path.join(workdir, prefix)
    nbatches = len(batches)

    fname = stem+'.py'
    _silentremove(fname)
    fout = open(fname, 'w')
    fout.write("""
import sys
del sys.path[:]
sys.path.append('"""+os.path.abspath(workdir) \
                            .encode('unicode_escape') \
                            .decode('utf-8')+"')\n")
    for p in sys.path:
        if p == '':
            p = os.path.abspath(os.getcwd())
        else:
            p = os.path.abspath(p)
        fout.write("sys.path.append('"+p.encode('unicode_escape') \
                                        .decode('utf-8')+"')\n")
    if prelude is not None:
        fout.write('\n'+prelude+'\n')
    fout.write("""
from cloudpickle import dump, load

fin = open(sys.argv[1], 'rb')
f = load(fin)
fin.close()

fin = open(sys.argv[2], 'rb')
xvals = load(fin)
fin.close()

fvals = [f(*x) for x in xvals]

fout = open(sys.argv[3], 'wb')
xvals = dump(fvals, fout)
fout.close()
    """)

    fname = stem+'.fn'
    _silentremove(fname)
    fout = open(fname, 'wb')
    dump(f, fout)
    fout.close()

    for i, xvals in enumerate(batches):
        fname = stem+'.i'+str(i)
        _silentremove(fname)
        fout = open(fname, 'wb')
        dump(xvals, fout)
        fout.close()

    for i in range(nbatches):
        _silentremove(stem+'.r'+str(i))
        if not append:
            _silentremove(stem+'.o'+str(i))
            _silentremove(stem+'.e'+str(i))

    fails = [0]*nbatches
    finished = [False]*nbatches
    running = [False]*nbatches
    results = [None]*nbatches
    runners = [runner(prefix, i, workdir, prefix+'.py',
                      [prefix+'.fn', prefix+'.i'+str(i), prefix+'.r'+str(i)],
                      prefix+'.o'+str(i), prefix+'.e'+str(i),
                      options=options, log=log, loglevel=loglevel) \
               for i in range(nbatches)]

    cancel = False
    oldnpending = -1
    oldnrunning = -1
    starttime = datetime.now()
    time_exceeded = False
    while not all(finished) and not cancel and not time_exceeded:
        time.sleep(refresh)
        for i in range(nbatches):
            if fails[i] >= tries:
                continue
            runners[i].poll()
            if runners[i].finished:
                if not os.path.exists(stem+'.r'+str(i)):
                    time.sleep(refresh)
                    if not os.path.exists(stem+'.r'+str(i)):
                        fails[i] += 1
                        if fails[i] >= tries:
                            if log is not None:
                                log.write('Lost job '+str(i)+ \
                                          ' (PID '+str(runners[i].pid)+ \
                                          '). Tries exhausted. Giving up.\n')
                                log.flush()
                            if autocancel:
                                cancel = True
                                break
                            else:
                                finished[i] = True
                                running[i] = False
                                results[i] = np.empty(len(batches[i]),
                                                      dtype=object)
                        else:
                            if log is not None:
                                log.write('Lost Job '+str(i)+' (PID '+ \
                                          str(runners[i].pid)+ \
                                          '). Restarting.\n')
                                log.flush()
                            runners[i].cancel()
                            runners[i] = runner(prefix, i, workdir, prefix+'.py',
                                                [prefix+'.fn',
                                                 prefix+'.i'+str(i),
                                                 prefix+'.r'+str(i)],
                                                prefix+'.o'+str(i),
                                                prefix+'.e'+str(i))
                else:
                    fin = open(stem+'.r'+str(i), 'rb')
                    results[i] = load(fin)
                    fin.close()
                    finished[i] = True
                    running[i] = False
            elif runners[i].running:
                running[i] = True

        nrunning = sum(running)
        npending = nbatches - nrunning - sum(finished)
        if nrunning != oldnrunning or npending != oldnpending:
            if log is not None:
                log.write('Pending: '+str(npending)+ \
                          ', Running: '+str(nrunning)+'\n')
                log.flush()
        oldnrunning = nrunning
        oldnpending = npending

        if timeout is not None and \
           (datetime.now() - starttime).total_seconds() > timeout:
            time_exceeded = True

    if cancel or time_exceeded:
        for i in range(nbatches):
            runners[i].cancel()
    if cancel:
        raise RuntimeError('Parallel execution failed')
    if time_exceeded:
        raise TimeoutError('Parallel execution timed out')

    if cleanup > 0:
        _silentremove(stem+'.py')
        _silentremove(stem+'.fn')
    for i in range(nbatches):
        if not autocancel and fails[i] >= tries:
            continue
        if cleanup > 0:
            _silentremove(stem+'.i'+str(i))
        if cleanup > 1:
            _silentremove(stem+'.r'+str(i))

        fname = stem+'.o'+str(i)
        if cleanup > 1 or \
           (os.path.isfile(fname) and os.path.getsize(fname) == 0):
                _silentremove(fname)

        fname = stem+'.e'+str(i)
        if os.path.isfile(fname) and os.path.getsize(fname) == 0:
            os.remove(fname)

    return results


class ParallelFunction:
    """Class for parallel function evaluation.

    Note:
      ParallelFunction objects are callable and accept a numpy array or list
      of tuples as argument. They parallelise the task of applying
      a certain function `f` to each tuple in the array. The tuples are
      'unpacked' when passed to the function `f`. Process communication
      is done via files and the parallel jobs can be run on different nodes of
      a computing cluster.

    Args:
      f (callable): Function to parallelise. It can take an arbitrary number
        of positional arguments but no keyword arguments.
      njobs (int, optional): Number of parallel jobs. Defaults to 2.
      batchsize (int or None): Number of evaluations done sequentially in each
        job. If not ``None`` this overrides the value of `njobs`, since
        the ParallelFunction object will create as many jobs as needed to achieve
        the desired batch size. Defaults to ``None``.
      workdir (str, optional): Directory where the parallel jobs are executed
        and where the temporary files for process communication are kept.
        Defaults to ``'.'``.
      prefix (str or None): Prefix to use for temporary files. Defaults to
        ``None``, in which case a unique file name is generated automatically.
      prelude (str or None): Python code which is inserted at the start of
        generated Python scripts. You can use this to adjust ``sys.path``,
        set a global random seed etc.
      runner (subclass of BasicRunner, optional): Class (*not* instance) which
        defines the details of the parallelisation. You can choose from the
        pre-defined classes :py:class:`~my_fitter.LocalRunner` and
        :py:class:`~my_fitter.SlurmRunner` or define your own by deriving from
        :py:class:`~my_fitter.BasicRunner`. Defaults to
        :py:class:`~my_fitter.LocalRunner`.
      tries (int, optional): Maximal number of attempts to re-start a crashed
        job. Defaults to 1.
      log (output stream or None, optional): Output stream for log messages.
        Defaults to ``None``, in which case no messages are printed.
      loglevel (int, optional): Amount of information to log. Defaults to
        1, in which case only job statuses are logged.
      refresh (int, optional): Time interval (in seconds) in which status of
        running jobs is checked. Defaults to 1.
      cleanup (int, optional): Indicates how much of the temporary data will be
        deleted after all jobs have finished. Defaults to 2 in which case all
        files except non-empty error logs are removed.  If `cleanup` is 0 no
        files are removed.
      append (bool, optional): Whether output and error files of the
        sub-processes should be appended to. Defaults to ``False``.
      options (str or list of str, optional): Extra command line options for the
        batch system. Defaults to ``[]``.
      timeout (float or None, optional): Number of seconds after which parallel
        execution is terminated. Defaults to ``None``, in which case no time
        limit is applied.

    """
    def __init__(self, f, njobs=2, batchsize=None, workdir='.', prefix=None,
                 prelude=None, runner=LocalRunner, tries=1, log=None,
                 loglevel=1, refresh=1, cleanup=2, append=False,
                 autocancel=True, options=[], timeout=None):
        self.f = f
        self.njobs = njobs
        self.batchsize = batchsize
        self.workdir = workdir
        self.prefix = prefix
        self.prelude = prelude
        self.runner = runner
        self.tries = tries
        self.log = log
        self.loglevel = loglevel
        self.refresh = refresh
        self.cleanup = cleanup
        self.append = append
        self.autocancel = autocancel
        self.options = options
        self.timeout = timeout

    def __call__(self, xx):
        if not (isinstance(xx, numpy.ndarray) or \
                (hasattr(xx, '__add__') and hasattr(xx, '__len__'))):
            raise ValueError('Argument must be numpy.ndarray or list.')

        npoints = len(xx)
        if self.batchsize is not None:
            batchsize = self.batchsize
        else:
            batchsize = npoints//self.njobs

        nbatches = npoints//batchsize
        remainder = npoints % batchsize

        batches = []
        for i in range(nbatches):
            if i < remainder:
                batches.append(xx[i*(batchsize+1) : (i+1)*(batchsize+1)])
            else:
                batches.append(xx[remainder*(batchsize+1) + \
                                  (i-remainder)*batchsize : \
                                  remainder*(batchsize+1) + \
                                  (i-remainder+1)*batchsize])

        results = _parallelize(self.f, batches,
                               workdir=self.workdir,
                               prefix=self.prefix,
                               prelude=self.prelude,
                               runner=self.runner,
                               tries=self.tries,
                               log=self.log,
                               loglevel=self.loglevel,
                               refresh=self.refresh,
                               cleanup=self.cleanup,
                               append=self.append,
                               autocancel=self.autocancel,
                               options=self.options,
                               timeout=self.timeout)

        if isinstance(xx, numpy.ndarray):
            return numpy.concatenate(results)
        else:
            return list(itertools.chain.from_iterable(results))


def parallel(**kwargs):
    """Decorator for parallelising a function.

    Note:
      Use as follows::

        @parallel(...)
        def myfunc(x):
            ...

      The arguments for ``parallel`` are the same as for
      :py:class:`~my_fitter.ParallelFunction`, but without the initial argument
      `f`. The defined function is parallelised and should only take a single
      argument.

    """
    return lambda f: ParallelFunction(f, **kwargs)

