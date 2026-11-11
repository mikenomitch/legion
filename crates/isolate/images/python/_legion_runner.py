"""The `python` image's runner: `legion:isolate/runner.run` for CPython.

componentize-py imports this module while it pre-initializes the image, so
everything at module level here (the interpreter, the `legion` module, the
stubs below, and the preloaded standard-library modules) is in the image's
memory snapshot and costs a run nothing.

Per run: `chdir("/legion/run")`, execute `request.entry` as `__main__`
with `sys.argv = [entry, *args]`, and report the exit status and the
top-level `result` variable. The `__main__` namespace belongs to the
instance: a `tenant` or `session` isolate keeps its globals from one run to
the next (a `request` isolate is a fresh instance every run).
"""

import builtins
import importlib
import json
import linecache
import os
import sys
import traceback
import types

import legion

from wit_world import exports
from wit_world.exports.runner import Outcome
from wit_world.imports import environment as _environment

RUN_DIR = "/legion/run"

# ---------------------------------------------------------------------------
# What an isolate cannot do: stubs that stop the run, by name.
# ---------------------------------------------------------------------------

PROCESSES = "processes are not available in an isolate"
SOCKETS = "sockets are not available in an isolate; use legion.fetch or run with --tier vm"
THREADS = "threads are not available in an isolate"
NATIVE = "native libraries are not available in an isolate"


def _stub(qualname, why):
    def stub(*args, **kwargs):
        legion.unsupported("%s: %s" % (qualname, why))

    stub.__name__ = qualname.rsplit(".", 1)[-1]
    stub.__qualname__ = qualname
    stub.__doc__ = "Not available in an isolate: %s." % why
    return stub


def _from_caller_code(frame):
    """True when `frame` runs the caller's code rather than the image's:
    the entry (compiled under its relative name), a module in the run
    directory, or text passed to exec/eval."""
    f = frame.f_code.co_filename
    if f.startswith("<"):
        return not f.startswith("<frozen")
    return not f.startswith("/") or f.startswith(RUN_DIR + "/")


def _install_stubs():
    import subprocess

    for name in ("run", "call", "check_call", "check_output", "getoutput", "getstatusoutput"):
        setattr(subprocess, name, _stub("subprocess." + name, PROCESSES))
    subprocess.Popen.__init__ = _stub("subprocess.Popen", PROCESSES)

    # CPython for WASI leaves most of these out of `os`, and the standard
    # library feature-tests them (`hasattr(os, "fork")` in random, logging,
    # socketserver, ...). A present name is replaced by a stub; an absent
    # one stays absent for the standard library and becomes a stub only
    # when the caller's own code (the entry, a file in the run directory,
    # exec'd text) reaches for it.
    absent = {}
    for name in (
        "system", "popen", "fork", "forkpty",
        "execl", "execle", "execlp", "execlpe", "execv", "execve", "execvp", "execvpe",
        "spawnl", "spawnle", "spawnlp", "spawnlpe", "spawnv", "spawnve", "spawnvp", "spawnvpe",
        "posix_spawn", "posix_spawnp",
    ):
        if hasattr(os, name):
            setattr(os, name, _stub("os." + name, PROCESSES))
        else:
            absent[name] = _stub("os." + name, PROCESSES)

    def os_getattr(name):
        stub = absent.get(name)
        if stub is not None and _from_caller_code(sys._getframe(1)):
            return stub
        raise AttributeError("module 'os' has no attribute %r" % name)

    os.__getattr__ = os_getattr

    import multiprocessing
    import multiprocessing.context
    import multiprocessing.process

    multiprocessing.process.BaseProcess.start = _stub("multiprocessing.Process.start", PROCESSES)
    for name in ("Pool", "Manager"):
        stub = _stub("multiprocessing." + name, PROCESSES)
        setattr(multiprocessing, name, stub)
        setattr(multiprocessing.context.BaseContext, name, stub)

    import concurrent.futures.process

    concurrent.futures.process.ProcessPoolExecutor.__init__ = _stub(
        "concurrent.futures.ProcessPoolExecutor", PROCESSES
    )

    import _thread
    import threading

    threading.Thread.start = _stub("threading.Thread.start", THREADS)
    _thread.start_new_thread = _stub("_thread.start_new_thread", THREADS)
    _thread.start_new = _thread.start_new_thread

    import socket

    socket.socket.__init__ = _stub("socket.socket", SOCKETS)
    for name in (
        "create_connection", "create_server", "socketpair", "fromfd",
        "getaddrinfo", "gethostbyname", "gethostbyname_ex", "gethostbyaddr",
    ):
        setattr(socket, name, _stub("socket." + name, SOCKETS))



# ---------------------------------------------------------------------------
# http.client and urllib.request over legion.fetch
# ---------------------------------------------------------------------------


class _Replay:
    """A socket stand-in holding a whole response, for http.client to parse."""

    def __init__(self, raw):
        self._raw = raw

    def makefile(self, mode="rb", *args, **kwargs):
        import io

        return io.BytesIO(self._raw)

    def close(self):
        pass


_HOP = {"transfer-encoding", "content-length", "connection", "keep-alive"}


def _install_http():
    import http
    import http.client as hc
    import socket

    plain_getresponse = hc.HTTPConnection.getresponse

    def request(self, method, url, body=None, headers={}, *, encode_chunked=False):
        if getattr(self, "_legion_request", None) is not None:
            raise hc.CannotSendRequest("a request is already pending on this connection")
        self._legion_request = (method, url, body, dict(headers))

    def getresponse(self):
        pending = getattr(self, "_legion_request", None)
        if pending is None:
            # The low-level putrequest/endheaders path: it needs a socket.
            return plain_getresponse(self)
        self._legion_request = None
        method, url, body, headers = pending
        https = isinstance(self, HTTPSConnection)
        if url.startswith(("http://", "https://")):
            full = url
        else:
            default = 443 if https else 80
            host = self.host if ":" not in self.host else "[%s]" % self.host
            authority = host if self.port == default else "%s:%d" % (host, self.port)
            full = "%s://%s%s" % ("https" if https else "http", authority, url or "/")
        if hasattr(body, "read"):
            body = body.read()
        elif isinstance(body, str):
            body = body.encode("iso-8859-1")
        elif body is not None and not isinstance(body, (bytes, bytearray, memoryview)):
            body = b"".join(c.encode("iso-8859-1") if isinstance(c, str) else bytes(c) for c in body)
        timeout = self.timeout if isinstance(self.timeout, (int, float)) else None
        r = legion.fetch(full, method=method, headers=headers, body=body, timeout=timeout)
        try:
            reason = http.HTTPStatus(r.status).phrase
        except ValueError:
            reason = ""
        lines = ["HTTP/1.1 %d %s" % (r.status, reason)]
        for k, v in r.headers:
            if k not in _HOP:
                lines.append("%s: %s" % (k, v))
        lines.append("content-length: %d" % len(r.body))
        raw = ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1") + r.body
        resp = hc.HTTPResponse(_Replay(raw), method=method, url=full)
        resp.begin()
        return resp

    hc.HTTPConnection.request = request
    hc.HTTPConnection.getresponse = getresponse
    hc.HTTPConnection.connect = _stub("http.client.HTTPConnection.connect", SOCKETS)

    if not hasattr(hc, "HTTPSConnection"):
        # CPython for WASI has no `ssl`, so http.client defines no
        # HTTPSConnection; TLS is the node's job here, so one that sends
        # through legion.fetch stands in.
        class HTTPSConnection(hc.HTTPConnection):
            default_port = hc.HTTPS_PORT

            def __init__(self, host, port=None, *, timeout=socket._GLOBAL_DEFAULT_TIMEOUT,
                         source_address=None, context=None, blocksize=8192):
                super().__init__(host, port, timeout, source_address, blocksize=blocksize)

        HTTPSConnection.__module__ = "http.client"
        hc.HTTPSConnection = HTTPSConnection
        hc.__all__.append("HTTPSConnection")
        hc._create_https_context = lambda http_version: None
    else:
        HTTPSConnection = hc.HTTPSConnection

    # Imported after HTTPSConnection exists, so urllib.request defines its
    # HTTPSHandler and every opener handles https.
    import urllib.request  # noqa: F401


# ---------------------------------------------------------------------------
# Image setup (runs once, while the image is pre-initialized)
# ---------------------------------------------------------------------------

_install_stubs()
_install_http()

# The standard library is only what is imported here. componentize-py
# snapshots the interpreter after this module ran and does not ship the
# library's files: at run time `/python` is not there, so a module that was
# not imported while the image was built cannot be imported at all. So the
# image imports every standard-library module and submodule that imports
# cleanly under WASI, minus the ones below (README.md, "The standard
# library").
SKIP = {
    # Not code a run can use: the test suite, IDLE, Tk, turtle demos,
    # installers, and modules that only print or open a browser.
    "test", "idlelib", "tkinter", "turtle", "turtledemo", "ensurepip", "venv",
    "pydoc_data", "antigravity", "this", "__hello__", "__phello__", "lib2to3",
    # The runner installs a stub in its place (below).
    "ctypes",
}
SKIP_PREFIXES = tuple(n + "." for n in SKIP)


def _preload():
    import pkgutil
    import warnings

    loaded, failed = [], []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        names = sorted(set(sys.stdlib_module_names) | set(sys.builtin_module_names))
        for name in names:
            if name in SKIP or name in sys.modules:
                continue
            try:
                importlib.import_module(name)
                loaded.append(name)
            except BaseException:
                failed.append(name)
        for name in names:
            mod = sys.modules.get(name)
            path = getattr(mod, "__path__", None)
            if not path or name in SKIP:
                continue
            for info in pkgutil.walk_packages(path, name + ".", onerror=lambda _n: None):
                sub = info.name
                if sub.startswith(SKIP_PREFIXES) or ".test" in sub or sub in sys.modules:
                    continue
                try:
                    importlib.import_module(sub)
                    loaded.append(sub)
                except BaseException:
                    failed.append(sub)
    return loaded, failed


PRELOADED, NOT_PRELOADED = _preload()


def _install_ctypes_stub():
    # `ctypes` is not built for WASI (no `_ctypes`). A stub package stands in
    # so that using it, not importing it, stops the run; it is left out of
    # the image's module list, so the pre-flight check still sends code
    # that imports it to a VM when the task has one.
    ctypes = types.ModuleType("ctypes", "Not available in an isolate: %s." % NATIVE)

    def _ctypes_getattr(name):
        if name.startswith("__"):
            raise AttributeError(name)
        legion.unsupported("ctypes.%s: %s" % (name, NATIVE))

    ctypes.__getattr__ = _ctypes_getattr
    sys.modules["ctypes"] = ctypes


# Installed after the preload: a standard-library module that tries ctypes
# while it is imported (_ios_support) then fails to import, as it would
# without the stub, instead of stopping the image build.
_install_ctypes_stub()

sys.stdout.reconfigure(line_buffering=True)
sys.stderr.reconfigure(line_buffering=True)

# Everything imported so far is shared, read-mostly image memory: keep the
# cyclic GC from walking (and so writing to) it in every instance.
import gc  # noqa: E402

gc.collect()
gc.freeze()

# ---------------------------------------------------------------------------
# Per instance
# ---------------------------------------------------------------------------

_fresh = True
_main = None


def _first_run():
    """What was frozen into the snapshot at build time and must be the
    instance's own: randomness and the environment."""
    import random
    import tempfile
    import uuid

    # `random`'s global generator was seeded while the image was built;
    # os.urandom is wasi:random, fresh per instance.
    random.seed()
    uuid._last_timestamp = None
    tempfile._name_sequence = None
    try:
        env = _environment.get_environment()
    except Exception:
        env = []
    os.environ.clear()
    for k, v in env:
        os.environ[k] = v


def _forget_run_dir_modules():
    """The run directory is emptied between runs: modules imported from it
    belong to the run that imported them."""
    prefix = RUN_DIR + "/"
    for name, mod in list(sys.modules.items()):
        f = getattr(mod, "__file__", None)
        if isinstance(f, str) and f.startswith(prefix):
            del sys.modules[name]
    # importlib.invalidate_caches() would import importlib.metadata, which
    # the image does not have; the path finders' caches are what matter.
    for finder in list(sys.path_importer_cache.values()):
        if finder is not None and hasattr(finder, "invalidate_caches"):
            finder.invalidate_caches()


def _flush():
    for f in (sys.stdout, sys.stderr):
        try:
            f.flush()
        except Exception:
            pass


def _exit_code(code):
    if code is None:
        return 0
    if isinstance(code, bool):
        return int(code)
    if isinstance(code, int):
        return max(-(2**31), min(code, 2**31 - 1))
    print(code, file=sys.stderr)
    return 1


def _result(ns):
    if "result" not in ns:
        return None
    try:
        return json.dumps(ns["result"])
    except (TypeError, ValueError) as e:
        print("legion: the top-level `result` is not JSON-serializable (%s); it is not reported" % e,
              file=sys.stderr)
        return None


class Runner(exports.Runner):
    def run(self, req):
        try:
            return _run(req)
        except BaseException:
            # A failure in the runner itself, not in the caller's code.
            traceback.print_exc()
            _flush()
            return Outcome(1, None)


def _run(req):
    global _fresh, _main
    if _fresh:
        _first_run()
        _fresh = False
    try:
        os.chdir(RUN_DIR)
    except OSError:
        pass
    if RUN_DIR not in sys.path[:1]:
        if RUN_DIR in sys.path:
            sys.path.remove(RUN_DIR)
        sys.path.insert(0, RUN_DIR)
    _forget_run_dir_modules()

    if req.input:
        try:
            legion.input = json.loads(req.input)
        except ValueError:
            legion.input = req.input
    else:
        legion.input = None

    entry = req.entry
    if not entry:
        print("legion: no entry file was given: send the source as a file (main.py)", file=sys.stderr)
        _flush()
        return Outcome(2, None)
    sys.argv = [entry, *req.args]
    path = entry if entry.startswith("/") else RUN_DIR + "/" + entry
    try:
        with open(path, "rb") as f:
            source = f.read()
    except OSError as e:
        print("python: can't open file %r: [Errno %s] %s" % (path, e.errno, e.strerror), file=sys.stderr)
        _flush()
        return Outcome(2, None)

    if _main is None:
        _main = types.ModuleType("__main__")
    ns = _main.__dict__
    ns["__file__"] = path
    ns["__builtins__"] = builtins
    ns.pop("result", None)
    sys.modules["__main__"] = _main
    text = source.decode("utf-8", errors="replace")
    linecache.cache[entry] = (len(text), None, text.splitlines(True), entry)

    exit_code = 0
    try:
        code = compile(source, entry, "exec")
        exec(code, ns)
    except SystemExit as e:
        exit_code = _exit_code(e.code)
    except BaseException as e:
        tb = e.__traceback__
        # Drop this runner's own frame; a SyntaxError has no caller frames.
        if tb is not None and tb.tb_frame.f_code is _run.__code__:
            tb = tb.tb_next
        traceback.print_exception(type(e), e, tb)
        exit_code = 1
    result = _result(ns)
    _flush()
    return Outcome(exit_code, result)
