"""Progress tests use only disposable files, with a deliberately waiting read."""
import hashlib
import os
import threading
import unittest
from unittest.mock import patch

import test_deduplicator as fixtures

m = fixtures.m


class ProgressTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.DeduplicatorTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)

    def test_hash_heartbeat_continues_during_waiting_read(self):
        path, _ = self.fixture.pair(content=b'abcdefghijk')
        ready = threading.Event()
        messages = []
        actual_read = os.read
        waiting = True

        def log(template, *args):
            messages.append(template % args)
            ready.set()

        def read(fd, size):
            nonlocal waiting
            if waiting:
                waiting = False
                self.assertTrue(ready.wait(2), 'heartbeat did not run while the read waited')
            return actual_read(fd, size)

        progress = m.Progress(0.02)
        with path.open('rb') as handle:
            with patch.object(m.LOG, 'info', side_effect=log), patch.object(m.os, 'read', side_effect=read):
                with progress:
                    progress.start('Hashing SHA-256', path=path, total_bytes=11)
                    result = m.hash_fd(handle.fileno(), progress)
                    progress.report()
        self.assertEqual(result, hashlib.sha256(b'abcdefghijk').hexdigest())
        self.assertIn('no byte progress', messages[0])
        self.assertIn('0.0%', messages[0])
        self.assertIn('100.0%', messages[-1])
        self.assertIn(str(path), messages[0])
        self.assertIsNone(progress.thread)

    def test_comparison_counts_one_file_and_reports_both_paths(self):
        left, right = self.fixture.pair(content=b'abcdefghijk')
        progress = m.Progress(30)
        progress.start('Comparing duplicate contents', path=right, other=left, total_bytes=11)
        with left.open('rb') as a, right.open('rb') as b:
            m.compare_fds(a.fileno(), b.fileno(), progress)
        self.assertEqual(progress.state['done'], 11)
        with self.assertLogs(m.LOG, 'INFO') as logs:
            progress.report()
        self.assertIn('100.0%', logs.output[0])
        self.assertIn(str(left), logs.output[0])
        self.assertIn(str(right), logs.output[0])

    def test_report_escapes_filename_control_characters(self):
        progress = m.Progress(30)
        progress.start('Hashing', path='a\nb\t"c', total_bytes=10)
        with self.assertLogs(m.LOG, 'INFO') as logs:
            progress.report()
        self.assertIn('file="a\\nb\\t\\"c"', logs.output[0])
        self.assertNotIn('\n', logs.output[0])

    def test_disabled_monitor_starts_no_thread(self):
        progress = m.Progress(0)
        with patch.object(m.threading, 'Thread') as thread:
            with progress:
                progress.start('Hashing', total_bytes=10)
                progress.advance(10)
            thread.assert_not_called()
        self.assertIsNone(progress.state)

    def test_monitor_exits_on_interrupt_and_can_be_reused(self):
        progress = m.Progress(30)
        with self.assertRaises(KeyboardInterrupt):
            with progress:
                worker = progress.thread
                raise KeyboardInterrupt()
        self.assertFalse(worker.is_alive())
        with progress:
            worker = progress.thread
            self.assertTrue(worker.is_alive())
        self.assertFalse(worker.is_alive())

    def test_engine_monitor_exits_after_failure(self):
        engine = self.fixture.engine()
        with patch.object(engine, 'scan', side_effect=OSError('injected')):
            with self.assertRaises(OSError):
                engine.run()
        self.assertIsNone(engine.progress.thread)

    def test_hash_summary_counts_existing_hardlinks_once(self):
        source, _ = self.fixture.pair(content=b'abcdefghijk')
        os.link(source, source.with_name('alias.mkv'))
        engine = self.fixture.engine()
        with self.assertLogs(m.LOG, 'INFO') as logs:
            result = engine.run()
        self.assertEqual(result['inodes_hashed'], 2)
        self.assertEqual(result['bytes_hashed'], 22)
        output = '\n'.join(logs.output)
        self.assertIn('Scan complete: 3 files', output)
        self.assertIn('Hashing candidates: 3 paths; 2 unique inodes; 22.0 B', output)
        self.assertIn('Hashing complete: 2 inodes', output)

    def test_arguments_default_custom_disabled_and_invalid(self):
        self.assertEqual(m.arguments([]).progress_interval, 30)
        self.assertEqual(m.arguments(['--progress-interval', '10']).progress_interval, 10)
        self.assertEqual(m.arguments(['--progress-interval', '0']).progress_interval, 0)
        for value in ('-1', 'nan', 'inf', 'nope'):
            with self.subTest(value=value), patch('sys.stderr'), self.assertRaises(SystemExit):
                m.arguments(['--progress-interval', value])


if __name__ == '__main__':
    unittest.main()
