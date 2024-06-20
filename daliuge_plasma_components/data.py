#
#    ICRAR - International Centre for Radio Astronomy Research
#    (c) UWA - The University of Western Australia, 2015
#    Copyright by UWA (in the framework of the ICRAR)
#    All rights reserved
#
#    This library is free software; you can redistribute it and/or
#    modify it under the terms of the GNU Lesser General Public
#    License as published by the Free Software Foundation; either
#    version 2.1 of the License, or (at your option) any later version.
#
#    This library is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
#    Lesser General Public License for more details.
#
#    You should have received a copy of the GNU Lesser General Public
#    License along with this library; if not, write to the Free Software
#    Foundation, Inc., 59 Temple Place, Suite 330, Boston,
#    MA 02111-1307  USA
#

"""
Plasma IO and PlasmaDrops implementation

Originally in daliuge/daliuge-engine/data/drops/plasma.py
"""

import binascii
import io
import logging
import os
from typing import Optional

import numpy as np
import pyarrow
from dlg.data.drops.data_base import DataDROP
from dlg.data.io import DataIO
from dlg.meta import dlg_bool_param, dlg_string_param
from overrides import overrides
from pyarrow import plasma as plasma

from daliuge_plasma_components.apps import PlasmaFlightClient

logger = logging.getLogger(__name__)


class PlasmaIO(DataIO):
    """
    A shared-memory IO reader/writer implemented using plasma store
    memory buffers. Note: not compatible with PlasmaClient put()/get()
    which performs data pickling before writing.
    """

    _desc: plasma.PlasmaClient

    def __init__(
        self,
        object_id: plasma.ObjectID,
        plasma_path="/tmp/plasma",
        expected_size: Optional[int] = None,
        use_staging=False,
    ):
        """Initializer
        Args:
            object_id (plasma.ObjectID): 20 bytes unique object id
            plasma_path (str, optional): The socket file path visible to all shared processes. Defaults to "/tmp/plasma".
            expected_size (Optional[int], optional) Total size of data to allocate to buffer if known. Defaults to None.
            use_staging (bool, optional): Whether to stream first to a resizable staging buffer. Defaults to False.
        """
        super().__init__()
        self._plasma_path = plasma_path
        self._object_id = object_id
        self._reader = None
        self._writer = io.BytesIO()
        # treat sizes <1 as None
        self._expected_size = (
            expected_size if expected_size and expected_size > 0 else None
        )
        self._buffer_size = 0
        self._use_staging = use_staging

    @overrides
    def _open(self, **kwargs):
        return plasma.connect(self._plasma_path)

    @overrides
    def _close(self, **kwargs):
        if self._writer:
            if self._use_staging:
                self._desc.put_raw_buffer(self._writer.getbuffer(), self._object_id)
                self._writer.close()
            else:
                self._desc.seal(self._object_id)
                self._writer.close()
        if self._reader:
            self._reader.close()

    def _read(self, count, **kwargs):
        if not self._reader:
            [data] = self._desc.get_buffers([self._object_id])
            self._reader = pyarrow.BufferReader(data)
        return self._reader.read1(count)

    @overrides
    def _write(self, data, **kwargs) -> int:
        """
        Writes data into the PlasmaIO reserved buffer.
        If use_staging is False and expected_size is None, only a single write is allowed.
        If use_staging is False and expected_size is > 0, multiple writes up to expected_size is allowed.
        If use_staging is True, any number of writes may occur with a small performance penalty.
        """
        # NOTE: data must be a collection of bytes for len to represent the buffer bytesize
        # assert isinstance(
        #     data, Union[memoryview, bytes, bytearray, pyarrow.Buffer].__bytes__()
        # )
        databytes = data.nbytes if isinstance(data, memoryview) else len(data)

        if not self._use_staging:
            # write directly into fixed size plasma buffer
            self._buffer_size = (
                self._expected_size if self._expected_size is not None else databytes
            )
            plasma_buffer = self._desc.create(self._object_id, self._buffer_size)
            self._writer = pyarrow.FixedSizeBufferWriter(plasma_buffer)
        if self._writer.tell() + databytes > self._buffer_size:
            raise IOError(
                "".join(
                    [
                        f"attempted to write {self._writer.tell() + databytes} ",
                        f"bytes to plasma buffer of size {self._buffer_size}, ",
                        "consider using staging or expected_size argument",
                    ]
                )
            )

        self._writer.write(data)
        return len(data)

    @overrides
    def _size(self, **kwargs) -> int:
        return self._buffer_size

    @overrides
    def exists(self) -> bool:
        return self._object_id in self._desc.list()

    @overrides
    def delete(self):
        self._desc.delete([self._object_id])

    @overrides
    def buffer(self) -> memoryview:
        [data] = self._desc.get_buffers([self._object_id])
        return memoryview(data)


class PlasmaFlightIO(DataIO):
    """
    A plasma drop managed by an arrow flight network protocol
    """

    _desc: PlasmaFlightClient

    def __init__(
        self,
        object_id: plasma.ObjectID,
        plasma_path="/tmp/plasma",
        flight_path: Optional[str] = None,
        expected_size: Optional[int] = None,
        use_staging=False,
    ):
        super().__init__()
        self._object_id = object_id
        self._plasma_path = plasma_path
        self._flight_path = flight_path
        self._reader = None
        self._writer = io.BytesIO()
        # treat sizes <1 as None
        self._expected_size = (
            expected_size if expected_size and expected_size > 0 else None
        )
        self._buffer_size = 0
        self._use_staging = use_staging

    def _open(self, **kwargs):
        return PlasmaFlightClient(socket=self._plasma_path)

    def _close(self, **kwargs):
        if self._writer:
            if self._use_staging:
                self._desc.put_raw_buffer(self._writer.getbuffer(), self._object_id)
                self._writer.close()
            else:
                if self._expected_size != self._writer.tell():
                    logger.debug(
                        f"written {self._writer.tell()} but expected {self._expected_size} bytes"
                    )
                self._desc.seal(self._object_id)
        if self._reader:
            self._reader.close()

    def _read(self, count, **kwargs):
        if not self._reader:
            data = self._desc.get_buffer(self._object_id, self._flight_path)
            self._reader = pyarrow.BufferReader(data)
        return self._reader.read1(count)

    def _write(self, data, **kwargs) -> int:

        # NOTE: data must be a collection of bytes for len to represent the buffer bytesize
        # assert isinstance(
        #     data, Union[memoryview, bytes, bytearray, pyarrow.Buffer].__args__
        # )
        databytes = data.nbytes if isinstance(data, memoryview) else len(data)
        if self._use_staging:
            # stream into resizeable buffer
            logger.warning(
                "Using dynamically sized Plasma buffer. Performance may be reduced."
            )
        else:
            # write directly to fixed size plasma buffer
            self._buffer_size = (
                self._expected_size if self._expected_size is not None else databytes
            )
            plasma_buffer = self._desc.create(self._object_id, self._buffer_size)
            self._writer = pyarrow.FixedSizeBufferWriter(plasma_buffer)
        self._writer.write(data)
        return len(data)

    @overrides
    def exists(self) -> bool:
        return self._desc.exists(self._object_id, self._flight_path)

    @overrides
    def _size(self, **kwargs) -> int:
        return self._buffer_size

    @overrides
    def delete(self):
        pass

    @overrides
    def buffer(self) -> memoryview:
        return self._desc.get_buffer(self._object_id, self._flight_path)


##
# @brief Plasma
# @details An object in a Apache Arrow Plasma in-memory object store
# @par EAGLE_START
# @param category Plasma
# @param tag daliuge
# @param plasma_path /String/ApplicationArgument/NoPort/ReadWrite//False/False/Path to the local plasma store
# @param object_id /String/ApplicationArgument/NoPort/ReadWrite//False/False/PlasmaId of the object for all compute nodes
# @param dropclass dlg.data.drops.plasma.PlasmaDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param use_staging False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Enables writing to a dynamically resizeable staging buffer
# @param dummy /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Dummy port
# @par EAGLE_END
class PlasmaDROP(DataDROP):
    """
    A DROP that points to data stored in a Plasma Store
    """

    object_id: bytes = dlg_string_param("object_id", None)
    plasma_path: str = dlg_string_param("plasma_path", "/tmp/plasma")
    use_staging: bool = dlg_bool_param("use_staging", False)

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        self.plasma_path = os.path.expandvars(self.plasma_path)
        if self.object_id is None:
            self.object_id = (
                np.random.bytes(20) if len(self.uid) != 20 else self.uid.encode("ascii")
            )
        elif isinstance(self.object_id, str):
            self.object_id = self.object_id.encode("ascii")

    def getIO(self):
        return PlasmaIO(
            plasma.ObjectID(self.object_id),
            self.plasma_path,
            expected_size=self._expectedSize,
            use_staging=self.use_staging,
        )

    @property
    def dataURL(self) -> str:
        return "plasma://%s" % (binascii.hexlify(self.object_id).decode("ascii"))


##
# @brief PlasmaFlight
# @details An Apache Arrow Flight server providing distributed access
# to a Plasma in-memory object store
# @par EAGLE_START
# @param category PlasmaFlight
# @param tag daliuge
# @param plasma_path /String/ApplicationArgument/NoPort/ReadWrite//False/False/Path to the local plasma store
# @param object_id /String/ApplicationArgument/NoPort/ReadWrite//False/False/PlasmaId of the object for all compute nodes
# @param dropclass dlg.data.drops.plasma.PlasmaFlightDROP/String/ComponentParameter/NoPort/ReadWrite//False/False/Drop class
# @param data_volume 5/Float/ConstraintParameter/NoPort/ReadWrite//False/False/Estimated size of the data contained in this node
# @param group_end False/Boolean/ComponentParameter/NoPort/ReadWrite//False/False/Is this node the end of a group?
# @param flight_path /String/ComponentParameter/NoPort/ReadWrite//False/False/IP and flight port of the drop owner
# @param dummy /Object/ApplicationArgument/InputOutput/ReadWrite//False/False/Dummy port
# @par EAGLE_END
class PlasmaFlightDROP(DataDROP):
    """
    A DROP that points to data stored in a Plasma Store
    """

    object_id: bytes = dlg_string_param("object_id", None)
    plasma_path: str = dlg_string_param("plasma_path", "/tmp/plasma")
    flight_path: str = dlg_string_param("flight_path", None)
    use_staging: bool = dlg_bool_param("use_staging", False)

    def initialize(self, **kwargs):
        super().initialize(**kwargs)
        self.plasma_path = os.path.expandvars(self.plasma_path)
        if self.object_id is None:
            self.object_id = (
                np.random.bytes(20) if len(self.uid) != 20 else self.uid.encode("ascii")
            )
        elif isinstance(self.object_id, str):
            self.object_id = self.object_id.encode("ascii")

    def getIO(self):
        return PlasmaFlightIO(
            plasma.ObjectID(self.object_id),
            self.plasma_path,
            flight_path=self.flight_path,
            expected_size=self._expectedSize,
            use_staging=self.use_staging,
        )

    @property
    def dataURL(self) -> str:
        return "plasmaflight://%s" % (binascii.hexlify(self.object_id).decode("ascii"))
