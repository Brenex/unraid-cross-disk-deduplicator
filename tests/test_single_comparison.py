"""Ensure replacement validation never rereads movie content after linking."""
import os
import unittest
from unittest.mock import patch

import test_deduplicator as fixtures

m = fixtures.m


class SingleComparisonTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.DeduplicatorTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def test_real_run_compares_once_and_never_reads_content_after_link_ready(self):
        source, old = self.fixture.pair()
        engine = self.fixture.engine(True)
        actual_record = engine.journal.write
        actual_read = os.read
        link_ready = False

        def record(event, **fields):
            nonlocal link_ready
            if event == 'LINK_READY':
                link_ready = True
            actual_record(event, **fields)

        def read(fd, size):
            self.assertFalse(link_ready, 'movie content reread after LINK_READY')
            return actual_read(fd, size)

        engine.journal.write = record
        with patch.object(m, 'compare_fds', wraps=m.compare_fds) as compare:
            with patch.object(m.os, 'read', side_effect=read):
                result = engine.run()
        self.assertEqual(compare.call_count, 1)
        self.assertEqual(result['completed'], 1)
        self.assertFalse(old.exists())
        self.assertTrue(os.path.samefile(source, self.fixture.a / 'data/media/renamed.mkv'))
        verified = [fields for event, fields in engine.journal.events if event == 'VERIFIED']
        self.assertEqual(verified[0]['verification'], 'inode-and-metadata')

    def test_dry_run_still_compares_once_and_preserves_both_files(self):
        source, old = self.fixture.pair()
        engine = self.fixture.engine()
        with patch.object(m, 'compare_fds', wraps=m.compare_fds) as compare:
            result = engine.run()
        self.assertEqual(compare.call_count, 1)
        self.assertEqual(result['planned'], 1)
        self.assertTrue(source.exists())
        self.assertTrue(old.exists())
        self.assertFalse((self.fixture.a / 'data/media/renamed.mkv').exists())

    def test_old_copy_change_after_link_ready_keeps_original_and_rolls_back_link(self):
        source, old = self.fixture.pair()
        engine = self.fixture.engine(True)
        actual_record = engine.journal.write

        def record(event, **fields):
            actual_record(event, **fields)
            if event == 'LINK_READY':
                old.write_bytes(b'CHANGED')  # Same size, different contents/metadata.

        engine.journal.write = record
        with self.assertRaises(m.SafetyError):
            engine.run()
        self.assertEqual(old.read_bytes(), b'CHANGED')
        self.assertTrue(source.exists())
        self.assertFalse((self.fixture.a / 'data/media/renamed.mkv').exists())

    def test_replacement_inode_substitution_keeps_original(self):
        source, old = self.fixture.pair()
        target = self.fixture.a / 'data/media/renamed.mkv'
        engine = self.fixture.engine(True)
        actual_record = engine.journal.write

        def record(event, **fields):
            actual_record(event, **fields)
            if event == 'LINK_READY':
                target.unlink()
                target.write_bytes(b'content')  # Matching bytes, wrong inode.

        engine.journal.write = record
        with self.assertRaises(m.SafetyError):
            engine.run()
        self.assertTrue(old.exists())
        self.assertTrue(source.exists())
        self.assertFalse(os.path.samefile(source, target))


if __name__ == '__main__':
    unittest.main()
