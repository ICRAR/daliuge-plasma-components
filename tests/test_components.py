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

import pytest
import unittest
import logging
import os

given = pytest.mark.parametrize

from dlg.apps.app_base import BarrierAppDROP
from dlg.droputils import DROPWaiterCtx, allDropContents

from crc32c import crc32c
from dlg.ddap_protocol import DROPStates
from dlg.data.drops.memory import InMemoryDROP

import subprocess

from daliuge_plasma_components.data import PlasmaDROP, PlasmaFlightDROP


class SumupContainerChecksum(BarrierAppDROP):
    """

    ===============================================================
    Class originally from daliuge/daliuge-engine/test/test_drop.py
    ===============================================================

    A dummy BarrierAppDROP that recursively sums up the checksums of
    all the individual DROPs it consumes, and then stores the final
    result in its output DROP
    """

    def run(self):
        crcSum = 0
        for inputDrop in self.inputs:
            if inputDrop.status == DROPStates.COMPLETED:
                if inputDrop.checksum:
                    crcSum += inputDrop.checksum
        outputDrop = self.outputs[0]
        outputDrop.write(str(crcSum).encode("utf8"))


class TestDROP(unittest.TestCase):
    """
    ===============================================================
    Class originally from daliuge/daliuge-engine/test/test_drop.py

    These methods have been moved here due to deprecation of arrow,
    but we want to have some way of storing them in case we decide
    to pick up the work to transition from plasma to an alternative.
    ===============================================================
    """

    def setUp(self):
        """
        library-specific setup
        """
        self._test_drop_sz = 16  # MB
        self._test_block_sz = 2  # MB
        self._test_num_blocks = self._test_drop_sz // self._test_block_sz
        self._test_block = os.urandom(self._test_block_sz * 1024**2)

    def _test_write_withDropType(self, dropType):
        """
        Test an AbstractDROP and a simple AppDROP (for checksum calculation)
        """
        a = dropType("oid:A", "uid:A", expectedSize=self._test_drop_sz * 1024**2)
        b = SumupContainerChecksum("oid:B", "uid:B")
        c = InMemoryDROP("oid:C", "uid:C")
        b.addInput(a)
        b.addOutput(c)

        test_crc = 0
        with DROPWaiterCtx(self, c):
            for _ in range(self._test_num_blocks):
                a.write(self._test_block)
                test_crc = crc32c(self._test_block, test_crc)

        # Read the checksum from c
        cChecksum = int(allDropContents(c))

        self.assertNotEqual(a.checksum, 0)
        self.assertEqual(a.checksum, test_crc)
        self.assertEqual(cChecksum, test_crc)

    def _test_dynamic_write_withDropType(self, dropType):
        """
        Test an AbstractDROP and a simple AppDROP (for checksum calculation)
        without an expected drop size (for app compatibility and not
        recommended in production)
        """
        # NOTE: use_staging required for multiple writes to plasma drops
        a = dropType("oid:A", "uid:A", expectedSize=-1, use_staging=True)
        b = SumupContainerChecksum("oid:B", "uid:B")
        c = InMemoryDROP("oid:C", "uid:C")
        b.addInput(a)
        b.addOutput(c)

        test_crc = 0
        with DROPWaiterCtx(self, c):
            for _ in range(self._test_num_blocks):
                a.write(self._test_block)
                test_crc = crc32c(self._test_block, test_crc)
            a.setCompleted()

        # Read the checksum from c
        cChecksum = int(allDropContents(c))

        self.assertNotEqual(a.checksum, 0)
        self.assertEqual(a.checksum, test_crc)
        self.assertEqual(cChecksum, test_crc)

    def test_write_plasmaDROP(self):
        """
        Test an PlasmaDrop and a simple AppDROP (for checksum calculation)
        """
        store = None
        try:
            store = subprocess.Popen(
                ["plasma_store", "-m", "100000000", "-s", "/tmp/plasma"]
            )
            self._test_write_withDropType(PlasmaDROP)
        except FileNotFoundError:
            logging.info(f"plasma_store not found when running test.")
        finally:
            if store:
                store.terminate()

    def test_dynamic_write_plasmaDROP(self):
        """
        Test an PlasmaDrop and a simple AppDROP (for checksum calculation)
        """
        store = None
        try:
            store = subprocess.Popen(
                ["plasma_store", "-m", "100000000", "-s", "/tmp/plasma"]
            )
            self._test_dynamic_write_withDropType(PlasmaDROP)
        except FileNotFoundError:
            logging.info(f"plasma_store not found when running test.")
        finally:
            if store:
                store.terminate()

    def test_write_plasmaFlightDROP(self):
        """
        Test an PlasmaDrop and a simple AppDROP (for checksum calculation)
        """
        store = None
        try:
            store = subprocess.Popen(
                ["plasma_store", "-m", "100000000", "-s", "/tmp/plasma"]
            )
            self._test_write_withDropType(PlasmaFlightDROP)
        except FileNotFoundError:
            logging.info(f"plasma_store not found when running test.")
        finally:
            if store:
                store.terminate()

    def test_dynamic_write_plasmaFlightDROP(self):
        """
        Test an PlasmaDrop and a simple AppDROP (for checksum calculation)
        """
        store = None
        try:
            store = subprocess.Popen(
                ["plasma_store", "-m", "100000000", "-s", "/tmp/plasma"]
            )
            self._test_dynamic_write_withDropType(PlasmaFlightDROP)
        except FileNotFoundError:
            logging.info(f"plasma_store not found when running test.")
        finally:
            if store:
                store.terminate()
