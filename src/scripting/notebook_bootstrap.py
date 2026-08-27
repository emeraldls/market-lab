"""Private bootstrap injected by `mlab notebook`; not a public Python package."""

import base64
import json
import os
import socket


_MAX_FRAME_BYTES = 4 * 1024 * 1024
_PAYLOAD_CHUNK_BYTES = 192 * 1024


def _write_frame(stream, value):
    payload = json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")
    if len(payload) > _MAX_FRAME_BYTES:
        raise RuntimeError("Market Lab notebook request frame is too large")
    stream.write(payload + b"\n")
    stream.flush()


def _read_frame(stream):
    payload = stream.readline(_MAX_FRAME_BYTES + 1)
    if not payload:
        raise RuntimeError("Market Lab notebook bridge closed the connection")
    if len(payload) > _MAX_FRAME_BYTES or not payload.endswith(b"\n"):
        raise RuntimeError("Market Lab notebook response frame is too large")
    try:
        return json.loads(payload)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("Market Lab notebook bridge returned invalid JSON") from error


class _Study:
    def __init__(self, bridge):
        self._bridge = bridge

    def sma(self, rows, options):
        return self._bridge._study("sma", rows, options)

    def ema(self, rows, options):
        return self._bridge._study("ema", rows, options)

    def cvd(self, rows, options):
        return self._bridge._study("cvd", rows, options)

    def spread(self, book):
        return self._bridge._study("spread", book, {})

    def depth(self, book, options=None):
        return self._bridge._study("depth", book, options or {})

    def imbalance(self, book, options=None):
        return self._bridge._study("imbalance", book, options or {})

    def slippage(self, book, options):
        return self._bridge._study("slippage", book, options)

    def vamp(self, book, options):
        return self._bridge._study("vamp", book, options)


class NotebookHistory:
    def __init__(self, bridge, from_ms, to_ms, from_utc, to_utc):
        self._bridge = bridge
        self._from_ms = from_ms
        self._to_ms = to_ms
        self._from_utc = from_utc
        self._to_utc = to_utc
        self._cache = {}

    @property
    def from_ms(self):
        return self._from_ms

    @property
    def to_ms(self):
        return self._to_ms

    def source(self, selector, index=None):
        if not isinstance(selector, str) or not selector.strip():
            raise TypeError("history.source selector must be a non-empty string")
        if index is not None and (isinstance(index, bool) or not isinstance(index, int) or index < 0):
            raise TypeError("history.source index must be a non-negative integer")
        if selector not in self._cache:
            self._cache[selector] = self._bridge._source(
                selector,
                self._from_ms,
                self._to_ms,
            )
        rows = self._cache[selector]
        if index is None:
            return rows
        return rows[-1 - index] if index < len(rows) else None

    def clear(self, selector=None):
        if selector is None:
            self._cache.clear()
            return
        self._cache.pop(selector, None)

    def __repr__(self):
        return (
            "MarketLabHistory("
            f"from_={self._from_utc!r}, to={self._to_utc!r}, "
            f"sources={len(self._cache)})"
        )


class MarketLabNotebook:
    def __init__(self, socket_path, token):
        self._socket_path = socket_path
        self._token = token
        self.study = _Study(self)

    def _connect(self):
        connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            connection.connect(self._socket_path)
        except Exception:
            connection.close()
            raise
        return connection

    def _read_result(self, stream):
        first = _read_frame(stream)
        if first.get("type") == "error":
            raise RuntimeError(first.get("message", "Market Lab notebook request failed"))
        if first.get("type") != "result_start":
            raise RuntimeError("Market Lab notebook bridge returned an unexpected response")
        chunks = []
        while True:
            frame = _read_frame(stream)
            frame_type = frame.get("type")
            if frame_type == "result_chunk":
                try:
                    chunks.append(base64.b64decode(frame["data"], validate=True))
                except Exception as error:
                    raise RuntimeError("Market Lab notebook result chunk is invalid") from error
                continue
            if frame_type == "result_end":
                break
            if frame_type == "error":
                raise RuntimeError(frame.get("message", "Market Lab notebook request failed"))
            raise RuntimeError("Market Lab notebook bridge returned an unexpected result frame")
        try:
            return json.loads(b"".join(chunks))
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise RuntimeError("Market Lab notebook result is invalid JSON") from error

    def history(self, *, from_, to):
        if not isinstance(from_, str) or not isinstance(to, str):
            raise TypeError("mlab.history from_ and to must be UTC date strings")
        with self._connect() as connection, connection.makefile(
            "rwb", buffering=0
        ) as stream:
            _write_frame(
                stream,
                {
                    "type": "history",
                    "token": self._token,
                    "from": from_,
                    "to": to,
                },
            )
            result = self._read_result(stream)
        return NotebookHistory(
            self,
            result["fromMs"],
            result["toMs"],
            result["fromUtc"],
            result["toUtc"],
        )

    def _source(self, selector, from_ms, to_ms):
        with self._connect() as connection, connection.makefile(
            "rwb", buffering=0
        ) as stream:
            _write_frame(
                stream,
                {
                    "type": "source",
                    "token": self._token,
                    "selector": selector,
                    "fromMs": from_ms,
                    "toMs": to_ms,
                },
            )
            first = _read_frame(stream)
            if first.get("type") == "error":
                raise RuntimeError(first.get("message", "Market Lab source request failed"))
            if first.get("type") != "source_start":
                raise RuntimeError("Market Lab notebook bridge returned an unexpected source response")
            rows = []
            while True:
                frame = _read_frame(stream)
                frame_type = frame.get("type")
                if frame_type == "source_chunk":
                    rows.extend(frame.get("records", []))
                    continue
                if frame_type == "source_end":
                    return rows
                if frame_type == "error":
                    raise RuntimeError(frame.get("message", "Market Lab source request failed"))
                raise RuntimeError("Market Lab notebook bridge returned an unexpected source frame")

    def _study(self, name, input_value, options):
        encoded = json.dumps(
            {"input": input_value, "options": options},
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        with self._connect() as connection, connection.makefile(
            "rwb", buffering=0
        ) as stream:
            _write_frame(
                stream,
                {"type": "study_start", "token": self._token, "name": name},
            )
            for offset in range(0, len(encoded), _PAYLOAD_CHUNK_BYTES):
                _write_frame(
                    stream,
                    {
                        "type": "study_chunk",
                        "token": self._token,
                        "data": base64.b64encode(
                            encoded[offset : offset + _PAYLOAD_CHUNK_BYTES]
                        ).decode("ascii"),
                    },
                )
            _write_frame(stream, {"type": "study_end", "token": self._token})
            return self._read_result(stream)

    def __repr__(self):
        return "MarketLabNotebook(connected=True, mode='research')"


def load_ipython_extension(ipython):
    socket_path = os.environ.get("MLAB_NOTEBOOK_SOCKET")
    token = os.environ.get("MLAB_NOTEBOOK_TOKEN")
    if not socket_path or not token:
        raise RuntimeError("Market Lab notebook session environment is missing")
    ipython.push({"mlab": MarketLabNotebook(socket_path, token)})
