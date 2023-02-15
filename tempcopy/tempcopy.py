import shutil
import os
import subprocess
import portalocker
import time
import sys
from multiprocessing import Process
from enum import Enum
from tempcopy import config

CacheStatus = Enum('CacheStatus', 'DNE DIR VALID STALE')

# Check status of cached dest file
def _check_cache(src, dest):
    if not os.path.exists(dest):
        return CacheStatus.DNE
    if os.path.isdir(dest):
        return CacheStatus.DIR
    assert(os.path.isfile(dest))
    src_modtime = os.stat(src).st_mtime
    dest_modtime = os.stat(dest).st_mtime
    if src_modtime > dest_modtime:
        return CacheStatus.STALE
    return CacheStatus.VALID

# Generate destination filename from src
# should deprecate in favor of user-facing `temppath`
def _gen_dest(src):
    dest = os.path.join(config['tempcopy_dir'], 
        os.path.splitdrive(os.path.abspath(src))[1].strip(os.path.sep))
    return dest

def temp_path(src):
    """
    Return the path used for temporary copies of `src`
    """
    return _gen_dest(src)

# Generate lockfile filename from dest
def _gen_lockfile(dest):
    return dest + '.lock'

# Copy src to dest using shutil.copystat
def _copy_shutil(src, dest):
    shutil.copy(src, dest)
    shutil.copystat(src, dest)
    
# Copy src to dest using gcp
def _copy_gcp(src, dest):
    
    # Check for gcp command
    gcp = shutil.which('gcp')
    if not gcp:
        raise EnvironmentError(
            'Could not find gcp. Do you need to module load it?')
        
    # Run
    p = subprocess.run([gcp, src, dest])
    time.sleep(10)
    
    # Check return value
    if p.returncode != 0:
        raise ChildProcessError(
            f'gcp failed for source {src}, destination {dest}')

# Lock-aware copy operation. Copies source to dest using a function set by
# copy_with. Interaction with existing copies is controlled by overwrite (see
# documentation for TmpCopy), and lock_on_copy controls whether a lock on
# lockfile has to be acquired before the copy operation can begin.
def _copy(source, dest, copy_with, overwrite, lock_on_copy, lockfile):
        
        # Make sure the path to the destination file exists
        # Must be done before attempting to acquire the lock so that the
        # directory that stores the lockfile can be created if needed
        head, tail = os.path.split(dest)
        os.makedirs(head, exist_ok=True)
        
        # Acquire the lockfile if necessary
        if lock_on_copy:
            f = open(lockfile, 'a')
            sys.stdout.flush()
            portalocker.lock(f, flags=portalocker.LOCK_EX)
            sys.stdout.flush()
        
        # Get status of cache and process cases
        cache_status = _check_cache(source, dest)
        
        # (1) Destination is a directory. This is an error
        if cache_status == CacheStatus.DIR:
            raise IsADirectoryError(f'Source file {source} maps to ' + 
                                    f'destination {dest}, which is a directory')
         
        # (2) Destination is valid and overwrite is not requested. This means
        # there's nothing left to do and the copy is already finished.
        if cache_status == CacheStatus.VALID and not overwrite:
            return
        
        # (3) Destination is stale or does not exist, or overwrite is requested.
        # In this case, actually copying is required
        if copy_with == 'shutil':
            fn = _copy_shutil
        elif copy_with == 'gcp':
            fn = _copy_gcp
        else:
            raise ValueError(f'{copy_with} is not a valid copying method')
        fn(source, dest)
        
        # Close and release the lockfile, if necessary
        if lock_on_copy:
            portalocker.unlock(f)
            f.close()

CopyStatus = Enum('CopyStatus', 'DELAYED IN_PROGRESS FINISHED')
"""
Status of the copy operation associated with a `TempCopy` instance.

Members
-------
CopyStatus.DELAYED
    Copy operation has not yet started. Most often encountered when a `TempCopy`
    is created with `copy_now=False`.
    
CopyStatus.IN_PROGRESS
    Copy operation is in progress. Most often encountered when a `TempCopy` is
    created with `copy_async=True`.
    
CopyStatus.FINISHED
    Copy operation is finished. The copy is safe to use if and only if the
    status field of a `TempCopy` is CopyStatus.FINISHED.
"""

class TempCopy:
    """
    An object that represents a temporary copy of a file. This class is fairly
    flexible, but is intended for working with files stored in a high-volume 
    but high-latency archive that must be copied to a temporary scratch
    directory before use.
    
    The user is responsible for specifying the source file; the destination is 
    determined automatically by appending the absolute path of the source file 
    to `xcrm.config['tempcopy_dir']`. Optional arguments control the method
    used to copy the file, whether copying occurs synchronously or 
    asynchronously, whether copying is started immediately or delayed,
    whether cached copies of the source file overwritten, and whether an
    (advisory) locking scheme is used to prevent multiple `TempCopy` instances
    from simultaneously trying to copy to the same destination file.
    
    Note that no attempt is made to clear up child processes (i.e., processes
    handling asynchronous copies) if their parent process (i.e., the process
    creating `TempCopy` instances) dies.
    """
    
    def __init__(
        self,
        source,
        copy_with='shutil',
        copy_async=False,
        copy_now=True,
        overwrite=True,
        lock_on_copy=True
    ):
        """
        Create a new `TempCopy` object from a source file.
        
        Parameters
        ----------
        source: str
            Absolute path to source file.
            
        copy_with: str or function, optional
            Method for copying files. Can be one of {'shutil', 'gcp'}. Future
            updates may add support for user-defined functions.
            
            * 'shutil': use `shutil.copy` and `shutil.copystat` to copy the 
              file and metadata. This is the only pre-defined method that avoids
              invoking an external program.
            * 'gcp': use the `gcp` program to copy the file. This is designed
              specifically for use on NOAA computer cluster and is likely to
              fail elsewhere.
              
        copy_async: bool, optional
            If True, run the copy operation asynchronously in a separate thread.
            Otherwise, run the copy operation in the current thread.
            
        copy_now: bool, optional
            If True, start the copy operation immediately. Otherwise, delay
            until `copy()` is called.
            
        overwrite: bool, optional
            If True, copy the source file even if it overwrites an existing file
            at the destination. Otherwise, only overwrite an existing file at
            the destination if the source file was modified more recently than
            the destination file.
            
        lock_on_copy: bool, optional
            If True, acquire a destination-specific lockfile before copying. The 
            locking scheme uses portalocker, which supports unix and windows 
            environments, but locking may need to be disabled on certain systems
            by setting `lock_on_copy=False`.
            
            Locks are used internally to prevent multiple `TempCopy` instances 
            from simultaneously copying to the same destination. However, they 
            do *not* prevent other processes from reading or writing to the
            destination file. This is true both on Unix, where file locks are
            advisory by default, and on Windows, where file locks are mandatory.
            The latter is because the locking scheme acquires and releases locks
            on a separate lockfile, not on the destination file (which may or
            may not exist when `copy` operations begin).
            
            When copying is asynchronous, locks are acquired and held by the
            *child* processes responsible for asynchronous copies.
            
        With default arguments `copy_async=False`, `copy_now=True`, and
        `overwrite=True`, creating a `TempCopy` immediately copies `source`, and
        the calling process blocks until the copy is complete. After completion,
        the `status` field of the created object is set to CopyStatus.FINISHED.
        
        With `copy_now=False`, the copy operation is delayed until the `copy`
        method of the `TempCopy` is invoked. The `status` field is initially set
        to `CopyStatus.DELAYED`.
        
        With `copy_async=False`, the copy operation is executed asynchronously
        in a separate process. (Whether it begins immediately or has to be
        manually triggered is still controlled by `copy_now`.) The `status`
        field is set to `CopyStatus.IN_PROGRESS` while the copy operation is
        executing. The `wait` method must be called before the destination file
        is used to ensure the copy operation finishes, and can be invoked
        directly or indirectly using the `path` method with 
        `not_finished='wait'`.
        
        Users are encouraged to use the `path` method rather than the `dest`
        attribute when accessing the copied file to avoid accessing a
        non-existent or incompletely-copied file.
        """
        
        self.source = source
        self.dest = _gen_dest(source)
        self.copy_with = copy_with
        self.copy_async = copy_async
        self.overwrite = overwrite
        self.lock_on_copy = lock_on_copy
        self.lockfile = _gen_lockfile(self.dest)
        self.status = CopyStatus.DELAYED
        if copy_now:
            self.copy()
            
    def copy(self):
        """
        Copy `self.source` to `self.dest`. If `self.copy_async`, begins the
        process to run the copy operation and then immediately returns.
        """
        
        self.status = CopyStatus.IN_PROGRESS
        
        # Most of the logic is in the _copy method. This method just 
        # creates a tuple of arguments ...
        args = (
            self.source, 
            self.dest, 
            self.copy_with,
            self.overwrite,
            self.lock_on_copy, 
            self.lockfile
        )
        
        # ... and decides whether to run _copy synchronously...
        if not self.copy_async:
            _copy(*args)
            self.status = CopyStatus.FINISHED
            return
        
        # ... or asynchronously
        self.p = Process(target=_copy, args=args)
        self.p.start()
        
    def wait(self):
        """
        If copying is asynchronous and in progress, call `join` on the copy
        process, check the exit code, and either raise an error (if the process
        returned a non-zero exit code) or set status to finished. The process is
        closed before the function returns. Otherwise, immediately return.
        """
        if not (self.copy_async and self.status == CopyStatus.IN_PROGRESS):
            return
        
        self.p.join()
        code = self.p.exitcode
        if code != 0:
            raise ChildProcessError(f'Copying {self.source} to {self.dest} ' +
                                    f'failed with exit code {code}')
        self.p.close()
        self.status = CopyStatus.FINISHED
        
    def path(self, not_finished='finish'):
        """
        Return the path to the destination file if the copy operation is
        finished, and take some other action if it's unfinished.
        
        Parameters
        ----------
        not_finished: str, optional
            Action to take if the copy operation isn't finished. Can be one of
            {'finish', 'error'}.
            
            * 'finish': call a combination of `copy` and `wait` to start and
              wait for the copy operation to finish, then return the path to
              the copied file.
              
            * 'error': raise a `FileNotFoundError`.
        """
        
        if not_finished not in ['error', 'finish']:
            raise ValueError(
                f"Invalid argument not_finished='{not_finished}'")
        
        if self.status == CopyStatus.FINISHED:
            return self.dest
        
        if not_finished == 'error':
            raise FileNotFoundError(
                f'Destination file {self.dest} has status {self.status}')
            
        if self.status == CopyStatus.DELAYED:
            self.copy()
        self.wait()
        assert(self.status == CopyStatus.FINISHED)
        return self.dest
    


# Used when copy_all encounters a path excluded by prefix
class _NoCopy:
    def __init__(self, path):
        self._path = path
    def path(self):
        return self._path
def _copy_all_nocopy(path):
    return _NoCopy(path)

# Used when copy_all encounters a path that requires copying
def _copy_all_copy(path, **kwargs):
    return TempCopy(path, **kwargs)

# Check if the start of path matches one of the prefixes
def _matches(path, prefix):
    if not prefix:
        return True
    return any([
        path.startswith(p)
        for p in prefix
    ])
    
def copy_all(paths, prefix=None, **kwargs):
    """
    Create temporary copies of all files in a collection of paths, optionally
    filtering by prefix.
    
    Parameters
    ----------
    paths: iterable of str
        Paths to files that require copying.
        
    prefix: iterable of str
        If provided, only copy paths that begin with one of the provided strings
        
    **kwargs:
        Passed on to `TempCopy`
        
    This function returns a list of objects, some of which may execute
    asynchronous copies, so `tempcopy.collect` should be called to obtain a list
    of paths to copies files after waiting, if necessary, for asynchronous
    copies to complete.
    """
    
    return [
        _copy_all_copy(path, **kwargs) if _matches(path, prefix)
        else _copy_all_nocopy(path)
        for path in paths
    ]
            
def collect(copies):
    """
    Return a list of paths to copied files after waiting, if necessary,
    for asynchronous copies to complete.
    
    Parameters
    ----------
    copies: output from `copy_all`
    """
    return [copy.path() for copy in copies]
 
def persist(write_function, dest, overwrite=True, copy_with='shutil'):
    """
    Create a new temporary file and a persistent copy at `src`.

    Parameters
    ----------
    write_function: str -> None 
        Function used to write the temporary file. It must take 
        a path as an argument and produce a file at that path.

    dest: str 
        Path to the persistent copy of the temporary file.
    
    overwrite: bool, optional 
        If True, overwrite existing files with the new temporary file 
        and persistent copy. If False, return without creating either 
        file if either exists already

    copy_with: str or function, optional 
        See documentation for `TempCopy`
    """

    # get location on fast scratch
    source = _gen_dest(dest)

    # make sure write directory exists 
    head, tail = os.path.split(source)
    os.makedirs(head, exist_ok=True)

    # check for existing files 
    if os.path.exists(dest) and not overwrite:
        raise ValueError(f'File already exists at persistent path {dest}')
    if os.path.exists(source) and not overwrite:
        raise ValueError(f'File already exists at temporary path {source}')

    # write temporary file 
    write_function(source)

    # copy temporary file to persistent storage
    # overwrite argument shouldn't alter behavior because of previous checks
    # no locking, so lock_on_copy is False and just pass /dev/null as lock file 
    _copy(source, dest, copy_with, overwrite, False, '/dev/null')
    

if __name__ == '__main__':
        
    f = TempCopy('/work/Tristan.Abbott/22loc/data/dyamond/regions.nc',
                 copy_with='gcp', copy_async=True, overwrite=True)
    f2 = TempCopy('/work/Tristan.Abbott/22loc/data/dyamond/regions.nc',
                  copy_with='gcp', copy_async=True, overwrite=False)
    f3 = TempCopy('/work/Tristan.Abbott/22loc/data/dyamond/zs.nc',
                 copy_with='gcp', copy_async=True, overwrite=True)
    print(f.status)
    print(f2.status)
    print(f3.status)
    
    print(f.path())
    print(f2.path())
    print(f3.path())
    
    print(f.status)
    print(f2.status)
    print(f3.status)

    def my_write(path):
        f = open(path, 'w')
        f.write('tempcopy persist test')
        f.close()

    persist(my_write, '/work/Tristan.Abbott/test.txt', copy_with='gcp')
