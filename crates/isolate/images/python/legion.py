"""Legion's API for Python code running in an isolate (the `python` image).

    import legion

    legion.input            # the run's JSON input, parsed (None when there is none)
    legion.info()           # {"job", "task", "lifetime", "tenant", "session", "resumed"}
    legion.park()           # a session isolate may be hibernated after this call
    legion.state.get(key)   # bytes or None; .set(key, bytes|str), .delete(key), .keys()
    legion.fetch(url, ...)  # outbound HTTP through the node's egress policy
    legion.vm.acquire()     # a child VM of the task's VM task; .attach(id)

Errors are `legion.StateError`, `legion.VmError` and `legion.FetchError`,
each with a `.kind`. `legion.Unsupported` is raised only if the host ever
returns from `control.unsupported`, which it does not: an unsupported call
stops the run.

The module is part of the image, imported while the image is
pre-initialized; the runner sets `legion.input` before each run.
"""

import json as _json

from componentize_py_types import Err as _Err
from wit_world.imports import control as _control
from wit_world.imports import state as _state
from wit_world.imports import vm as _vm
from wit_world.imports import types as _http
from wit_world.imports import outgoing_handler as _outgoing
from wit_world.imports import streams as _streams

__all__ = [
    "input",
    "info",
    "park",
    "state",
    "vm",
    "fetch",
    "Response",
    "StateError",
    "VmError",
    "FetchError",
    "Unsupported",
    "unsupported",
]

#: The run's input, parsed from JSON; None when the run has none. Set by the
#: runner before each run.
input = None


class Unsupported(Exception):
    """Something an isolate cannot do. The run is normally stopped before
    this is raised (see `unsupported`)."""


def unsupported(what):
    """Stop the run as `unsupported`, with `what` as the reason the caller
    sees (`stopped.detail`). Does not return."""
    import sys

    for f in (sys.stdout, sys.stderr):
        try:
            f.flush()
        except Exception:
            pass
    _control.unsupported(what)
    raise Unsupported(what)


_LIFETIMES = {0: "request", 1: "tenant", 2: "session"}


def info():
    """Who this isolate is: a dict with `job`, `task`, `lifetime`
    ("request", "tenant" or "session"), `tenant` and `session` (None when
    not set), and `resumed` (True on the first run after a wake from
    hibernation: memory is fresh and only `legion.state` survived)."""
    i = _control.info()
    return {
        "job": i.job,
        "task": i.task,
        "lifetime": _LIFETIMES.get(i.lifetime.value, str(i.lifetime)),
        "tenant": i.tenant,
        "session": i.session,
        "resumed": i.resumed,
    }


def park():
    """Tell the host this session isolate is waiting and may be hibernated
    when the current run returns. A no-op for request and tenant isolates."""
    _control.park()


# ---------------------------------------------------------------------------
# state
# ---------------------------------------------------------------------------


class StateError(Exception):
    """A `legion.state` call failed. `kind` is "not-available" (a request
    isolate keeps no state) or "quota" (`quota` holds the limit in bytes)."""

    def __init__(self, kind, message, quota=None):
        super().__init__(message)
        self.kind = kind
        self.quota = quota


def _state_call(fn, *args):
    try:
        return fn(*args)
    except _Err as e:
        v = e.value
        if isinstance(v, _state.StateError_Quota):
            raise StateError(
                "quota",
                "legion.state: the write would exceed the state quota of %d bytes" % v.value,
                quota=v.value,
            ) from None
        raise StateError(
            "not-available",
            'legion.state: a request isolate keeps no state (use lifetime = "tenant" or "session")',
        ) from None


class _State:
    """Bytes that outlive the instance (the session's, or the tenant's)."""

    def get(self, key):
        """The bytes stored under `key`, or None."""
        return _state_call(_state.get, str(key))

    def set(self, key, value):
        """Store `value` (bytes, or a str stored as UTF-8) under `key`."""
        if isinstance(value, str):
            value = value.encode("utf-8")
        elif not isinstance(value, (bytes, bytearray, memoryview)):
            raise TypeError("legion.state.set: value must be bytes or str, not %s" % type(value).__name__)
        _state_call(_state.set, str(key), bytes(value))

    def delete(self, key):
        """Remove `key` (no error when it is absent)."""
        _state_call(_state.delete, str(key))

    def keys(self):
        """Every key, as a list of str."""
        return list(_state_call(_state.keys))


state = _State()


# ---------------------------------------------------------------------------
# fetch
# ---------------------------------------------------------------------------


class FetchError(OSError):
    """An outbound request failed before a response arrived. `kind` is the
    `wasi:http` error code, for example "http-request-denied" when the
    task's egress policy refuses the destination."""

    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


class Response:
    """An HTTP response: `status`, `headers` (a list of (name, value) str
    pairs, names lower-case), `body` (bytes), `.text()`, `.json()`,
    `.header(name)`."""

    def __init__(self, status, headers, body, url=None):
        self.status = status
        self.headers = headers
        self.body = body
        self.url = url

    @property
    def ok(self):
        return 200 <= self.status < 300

    def header(self, name, default=None):
        name = name.lower()
        for k, v in self.headers:
            if k == name:
                return v
        return default

    def text(self, encoding="utf-8"):
        return self.body.decode(encoding, errors="replace")

    def json(self):
        return _json.loads(self.body)

    def __repr__(self):
        return "<legion.Response %d, %d bytes>" % (self.status, len(self.body))


def _kebab(name):
    out = []
    for i, c in enumerate(name):
        if c.isupper() and i:
            out.append("-")
        out.append(c.lower())
    return "".join(out)


def _error_kind(code):
    name = type(code).__name__
    return _kebab(name.split("_", 1)[1] if "_" in name else name)


_ERROR_TEXT = {
    "http-request-denied": "the task's egress policy does not allow this destination",
    "destination-not-found": "the host name did not resolve",
    "connection-refused": "the connection was refused",
    "connection-read-timeout": "the request timed out",
    "destination-unavailable": "the destination is unavailable",
}

_METHODS = {
    "GET": _http.Method_Get,
    "HEAD": _http.Method_Head,
    "POST": _http.Method_Post,
    "PUT": _http.Method_Put,
    "DELETE": _http.Method_Delete,
    "CONNECT": _http.Method_Connect,
    "OPTIONS": _http.Method_Options,
    "TRACE": _http.Method_Trace,
    "PATCH": _http.Method_Patch,
}

_CHUNK = 4096


def _header_pairs(headers):
    if headers is None:
        return []
    items = headers.items() if hasattr(headers, "items") else headers
    out = []
    for k, v in items:
        if isinstance(v, str):
            v = v.encode("latin-1")
        out.append((str(k).lower(), bytes(v)))
    return out


def _split_url(url):
    from urllib.parse import urlsplit

    u = urlsplit(url)
    scheme = u.scheme.lower()
    if scheme not in ("http", "https"):
        raise ValueError("legion.fetch: only http and https URLs are supported, not %r" % url)
    if not u.netloc:
        raise ValueError("legion.fetch: the URL has no host: %r" % url)
    path = u.path or "/"
    if u.query:
        path += "?" + u.query
    return scheme, u.netloc, path


def fetch(url, method="GET", headers=None, body=None, timeout=None):
    """Send one HTTP request and return a `Response`.

    `headers` is a dict or a list of pairs; `body` is bytes or str (sent as
    UTF-8). `timeout` is in seconds (connect and first byte); the run's own
    wall limit bounds it either way. The request goes through the node's
    egress policy: `http://<upstream>/...` reaches a `connect` upstream with
    its credentials added by the node. Raises `FetchError` when no response
    arrives.
    """
    scheme, authority, path = _split_url(url)
    if isinstance(body, str):
        body = body.encode("utf-8")
    req = _http.OutgoingRequest(_http.Fields.from_list(_header_pairs(headers)))
    m = method.upper()
    req.set_method(_METHODS[m]() if m in _METHODS else _http.Method_Other(method))
    req.set_scheme(_http.Scheme_Https() if scheme == "https" else _http.Scheme_Http())
    req.set_authority(authority)
    req.set_path_with_query(path)
    out_body = req.body()
    if body:
        stream = out_body.write()
        view = memoryview(bytes(body))
        for i in range(0, len(view), _CHUNK):
            stream.blocking_write_and_flush(bytes(view[i : i + _CHUNK]))
        stream.__exit__(None, None, None)
    options = None
    if timeout is not None:
        ns = int(timeout * 1e9)
        options = _http.RequestOptions()
        options.set_connect_timeout(ns)
        options.set_first_byte_timeout(ns)
    try:
        future = _outgoing.handle(req, options)
        _http.OutgoingBody.finish(out_body, None)
        while True:
            got = future.get()
            if got is not None:
                break
            future.subscribe().block()
        # Ok(Ok(response)) | Ok(Err(error-code)) | Err(()) (already taken)
        if isinstance(got, _Err):
            raise _Err(None)
        inner = got.value
        if isinstance(inner, _Err):
            raise inner
        resp = inner.value
    except _Err as e:
        code = e.value
        kind = _error_kind(code) if code is not None else "internal-error"
        why = _ERROR_TEXT.get(kind, kind)
        raise FetchError(kind, "legion.fetch %s %s: %s" % (m, url, why)) from None
    status = resp.status()
    hdrs = [(k, v.decode("latin-1")) for k, v in resp.headers().entries()]
    incoming = resp.consume()
    stream = incoming.stream()
    chunks = []
    while True:
        try:
            chunks.append(stream.blocking_read(65536))
        except _Err as e:
            if isinstance(e.value, _streams.StreamError_Closed):
                break
            raise FetchError("http-protocol-error", "legion.fetch %s %s: the response body failed" % (m, url)) from None
    stream.__exit__(None, None, None)
    _http.IncomingBody.finish(incoming)
    return Response(status, hdrs, b"".join(chunks), url=url)


# ---------------------------------------------------------------------------
# vm
# ---------------------------------------------------------------------------


class VmError(Exception):
    """A `legion.vm` call failed. `kind` is one of "not-configured" (the
    task has no VM task), "resource-exhausted" (retryable: the VM task's
    envelope is full), "child-gone" (stopped, evicted, or its node was
    lost), "needs-restart" (a resize past what the running VM can do), or
    "failed"."""

    def __init__(self, kind, message):
        super().__init__(message)
        self.kind = kind


_VM_KINDS = {
    "VmError_NotConfigured": "not-configured",
    "VmError_ResourceExhausted": "resource-exhausted",
    "VmError_ChildGone": "child-gone",
    "VmError_NeedsRestart": "needs-restart",
    "VmError_Failed": "failed",
}


def _vm_call(what, fn, *args):
    try:
        return fn(*args)
    except _Err as e:
        v = e.value
        kind = _VM_KINDS.get(type(v).__name__, "failed")
        detail = getattr(v, "value", None)
        if kind == "not-configured":
            msg = "legion.vm.%s: this task has no VM task (set config.vm.task)" % what
        else:
            msg = "legion.vm.%s: %s%s" % (what, kind, ": " + detail if detail else "")
        raise VmError(kind, msg) from None


class ExecResult:
    """What `Machine.exec` returns: `exit`, `stdout`, `stderr` (bytes),
    `timed_out`."""

    __slots__ = ("exit", "stdout", "stderr", "timed_out")

    def __init__(self, exit, stdout, stderr, timed_out):
        self.exit = exit
        self.stdout = stdout
        self.stderr = stderr
        self.timed_out = timed_out

    def __repr__(self):
        return "<legion.vm.ExecResult exit=%d timed_out=%s>" % (self.exit, self.timed_out)


def _b(data, what):
    if isinstance(data, str):
        return data.encode("utf-8")
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)
    raise TypeError("%s: data must be bytes or str, not %s" % (what, type(data).__name__))


class Machine:
    """A child VM. Keep `id` in `legion.state` to re-attach with
    `legion.vm.attach(id)` after hibernation. Dropping the object does not
    end the VM; `release()` does, and so does the end of its owner."""

    def __init__(self, handle):
        self._h = handle
        self.id = handle.id()

    def exec(self, argv, stdin=b"", timeout_ms=60000):
        """Run `argv` in the VM; returns an `ExecResult`."""
        if isinstance(argv, str):
            raise TypeError("legion.vm exec: argv is a list of str, e.g. ['sh', '-c', 'ls']")
        r = _vm_call("exec", self._h.exec, [str(a) for a in argv], _b(stdin, "exec stdin"), int(timeout_ms))
        return ExecResult(r.exit, r.stdout, r.stderr, r.timed_out)

    def put(self, path, data):
        """Write a file in the VM."""
        _vm_call("put", self._h.put, str(path), _b(data, "legion.vm put"))

    def get(self, path):
        """Read a file from the VM, as bytes."""
        return _vm_call("get", self._h.get, str(path))

    def fetch(self, port, method="GET", path="/", headers=None, body=b""):
        """An HTTP request to a port the VM listens on; returns a
        `legion.Response`."""
        req = _vm.HttpRequest(method.upper(), path, _header_pairs(headers), _b(body or b"", "legion.vm fetch"))
        r = _vm_call("fetch", self._h.fetch, int(port), req)
        hdrs = [(k.lower(), v.decode("latin-1")) for k, v in r.headers]
        return Response(r.status, hdrs, r.body)

    def resize(self, memory_mib, memory_max_mib=None, cpu_mhz=None):
        """Grow or shrink the VM (admitted against its task's envelope)."""
        _vm_call("resize", self._h.resize, _vm.Size(int(memory_mib), memory_max_mib, cpu_mhz))

    def release(self):
        """End the VM now."""
        self._h.release()

    def __repr__(self):
        return "<legion.vm.Machine %s>" % self.id


class _Vm:
    VmError = VmError
    Machine = Machine
    ExecResult = ExecResult

    def acquire(self):
        """Start a child VM of the task's VM task."""
        return Machine(_vm_call("acquire", _vm.acquire))

    def attach(self, id):
        """Re-attach to a child VM this isolate (or its session) owns."""
        return Machine(_vm_call("attach", _vm.attach, str(id)))


vm = _Vm()
