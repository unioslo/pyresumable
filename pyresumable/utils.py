import os
from typing_extensions import Buffer # TODO: When upgrading dependencies to demand Python >=3.12, change `typing_extensions` to `collections.abc`

from . import aio

AsyncRawIOBase = aio.io.RawIOBase

from asyncio import to_thread

class SizeCappedFileReader:
    """
    "Readers" of file-like objects, such that reading a file with the "reader" is done as if the file was cut, or truncated, to start a the current reading position of the file, and end at a specified number of bytes (size) offset from said position.

    This class offers a convenience for when you want to apply Python's library functions -- which often require [a file-like object](https://docs.python.org/3/glossary.html#term-file-object), as it is called -- but to a _portion_ of a file. The portion is effectively determined by the current position (as obtained with the `tell` method) and a size value that in turn determines the end of the portion as offset from the position, both determining the "window" or the portion in the file as if the file was truncated to only include the content in the portion.
    """
    def __init__(self, file: AsyncRawIOBase, size: int):
        """
        :param file: A file-like object that will back this reader; reading starts at the current reading position of the file; the file is assumed to have been opened in "binary" mode / vend _bytes_ (not "text")
        :param size: Size of the portion to read from; the portion starts at the current reading position of `file` as per convention -- calling `read` which advances the position
        """
        self._file = file
        self._size = self._remaining = size
    # The following methods implement the `io.RawIOBase` protocol (co-routine equivalent), currently just enough to be passable to `hashlib.file_digest`
    async def read(self, /, size: int = -1) -> bytes:
        result = await self._file.read(self._remaining if size == -1 or self._remaining < size else size)
        self._remaining -= len(result)
        assert self._remaining >= 0
        return result
    def readable(self) -> bool:
        return self._file.readable()
    async def readinto(self, b: Buffer, /) -> int: # For efficient hashing etc
        size = await self._file.readinto(b)
        x = self._remaining - size
        if x < 0:
            size = self._remaining
            await self._file.seek(x, os.SEEK_CUR)
        self._remaining -= size
        assert self._remaining >= 0
        return size

async def file_digest(file: AsyncRawIOBase, digest, /, *, _bufsize=2**18):
    """
    Compute a digest of a file asynchronously, using `hashlib`.

    Keep in mind that for sufficiently small buffer sizes, the cost of `to_thread` will outweigh the benefit. For sizes like 256KiB (`2**18`) the benefit of `to_thread` should be obvious, however, although subject to hardware performance, of course.
    """
    hasher = hashlib.new(digest) if isinstance(digest, str) else digest()
    buf = bytearray(_bufsize)
    view = memoryview(buf)
    while size := await file.readinto(buf):
        await to_thread(hasher.update, view[:size])
    return hasher
