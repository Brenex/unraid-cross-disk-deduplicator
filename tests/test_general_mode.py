"""Directory mapping and selection policy on disposable Linux filesystems."""
import os
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import test_deduplicator as fixtures

m = fixtures.m


class GeneralModeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.DeduplicatorTests()
        self.fixture.setUp()
        self.addCleanup(self.fixture.tearDown)
        self.a, self.b = self.fixture.a, self.fixture.b
        # B has more free bytes but a lower free percentage than A.
        self.capacities = {self.a.stat().st_dev: (1000, 900), self.b.stat().st_dev: (10000, 2000)}

        def usage(fd):
            total, available = self.capacities[os.fstat(fd).st_dev]
            return SimpleNamespace(f_frsize=1, f_blocks=total, f_bfree=available, f_bavail=available)

        self.usage = patch.object(m.os, 'fstatvfs', side_effect=usage).start()
        self.addCleanup(patch.stopall)

    def engine(self, argv=(), real=False):
        args = m.arguments(list(argv))
        args.real_run = real
        engine = m.Engine(args, [self.a, self.b], journal=fixtures.Journal(),
                          policy=fixtures.Policy(), owner=(os.getuid(), os.getgid()))
        self.fixture.engines.append(engine)
        return engine

    def pair(self):
        return self.fixture.pair('books/Authors/A/original.pdf', 'books/Other/renamed.pdf')

    def test_logical_directory_expands_and_is_recursive(self):
        left, right = self.pair()
        unrelated = self.fixture.put(self.a, 'other/books/ignore.pdf')
        engine = self.engine(['--directory', '/mnt/user/books/'])
        engine.scan()
        self.assertEqual({entry.path for entry in engine.inventory}, {left, right})
        self.assertNotIn(unrelated, {entry.path for entry in engine.inventory})

    def test_general_real_run_retains_more_available_bytes_and_preserves_paths(self):
        left, right = self.pair()
        engine = self.engine(['--directory', '/mnt/user/books/'], real=True)
        with self.assertLogs(m.LOG, 'INFO') as logs:
            result = engine.run()
        target = self.b / left.relative_to(self.a)
        self.assertEqual(result['completed'], 1)
        self.assertFalse(left.exists())
        self.assertTrue(right.exists())
        self.assertTrue(os.path.samefile(right, target))
        self.assertIn('most available bytes among existing copies', '\n'.join(logs.output))
        removed = [fields for event, fields in engine.journal.events if event == 'ORIGINAL_REMOVED'][0]
        self.assertEqual(removed['source_initial_available_bytes'], 2000)
        self.assertEqual(removed['original_initial_available_bytes'], 900)
        self.assertEqual(self.usage.call_count, 2)

    def test_general_dry_run_keeps_files_and_creates_no_hardlink(self):
        left, right = self.pair()
        engine = self.engine(['/mnt/user/books'])
        result = engine.run()
        self.assertEqual(result['planned'], 1)
        self.assertTrue(left.exists())
        self.assertTrue(right.exists())
        self.assertFalse((self.b / left.relative_to(self.a)).exists())

    def test_same_relative_path_copies_do_not_collide(self):
        left, right = self.fixture.pair('books/a.pdf', 'books/a.pdf')
        result = self.engine(['/mnt/user/books'], real=True).run()
        self.assertEqual(result['completed'], 1)
        self.assertFalse(left.exists())
        self.assertEqual(right.read_bytes(), b'content')

    def test_no_torrent_names_are_required(self):
        self.pair()
        engine = self.engine(['--mode', 'general', '--directory', '/mnt/user/books'])
        result = engine.run()
        self.assertEqual(result['duplicate_groups'], 1)
        self.assertEqual(result['inodes_hashed'], 2)

    def test_same_disk_only_copies_are_left_alone(self):
        left = self.fixture.put(self.a, 'books/a.pdf')
        right = self.fixture.put(self.a, 'books/b.pdf')
        result = self.engine(['/mnt/user/books'], real=True).run()
        self.assertEqual(result['duplicate_groups'], 0)
        self.assertTrue(left.exists())
        self.assertTrue(right.exists())
        self.assertFalse(os.path.samefile(left, right))

    def test_equal_size_different_content_is_not_consolidated(self):
        left, right = self.pair()
        right.write_bytes(b'CHANGED')
        result = self.engine(['/mnt/user/books'], real=True).run()
        self.assertEqual(result['duplicate_groups'], 0)
        self.assertTrue(left.exists())
        self.assertTrue(right.exists())

    def test_scope_boundary_does_not_include_books_old(self):
        left, right = self.pair()
        self.fixture.put(self.b, 'books-old/extra.pdf')
        engine = self.engine(['/mnt/user/books'])
        engine.scan()
        self.assertEqual({entry.path for entry in engine.inventory}, {left, right})

    def test_missing_directory_on_one_disk_is_allowed(self):
        self.fixture.put(self.a, 'books/a.pdf')
        result = self.engine(['/mnt/user/books']).run()
        self.assertEqual(result['files_scanned'], 1)

    def test_missing_directory_on_all_disks_fails_closed(self):
        engine = self.engine(['/mnt/user/books'])
        with self.assertRaises(m.SafetyError):
            engine.run()
        self.assertFalse(any(event == 'WOULD_DEDUPLICATE' for event, _ in engine.journal.events))

    def test_overlapping_directories_are_walked_once(self):
        self.pair()
        engine = self.engine(['--directory', '/mnt/user/books', '--directory', '/mnt/user/books/Authors'])
        with patch.object(engine, 'walk', wraps=engine.walk) as walk:
            engine.scan()
        roots = [(args[0].path, args[1]) for args, _ in walk.call_args_list]
        self.assertEqual(roots.count((self.a, 'books/Authors')), 1)
        self.assertEqual(len(engine.inventory), 2)

    def test_physical_scope_is_pinned_to_named_disk(self):
        left, right = self.pair()
        engine = self.engine(['--directory', str(left.parent), '--directory', str(right.parent)])
        engine.scan()
        self.assertEqual({entry.path for entry in engine.inventory}, {left, right})
        self.assertEqual(engine.directory_scopes[0].root, str(self.a))

    def test_exclusions_apply_within_scoped_walks(self):
        left, right = self.pair()
        excluded = self.fixture.put(self.b, 'books/excluded/x.pdf')
        engine = self.engine(['/mnt/user/books', '-e', str(self.b / 'books/excluded')])
        engine.scan()
        self.assertEqual({entry.path for entry in engine.inventory}, {left, right})
        self.assertNotIn(excluded, {entry.path for entry in engine.inventory})

    def test_logical_exclusion_maps_to_every_array_disk(self):
        engine = self.engine(['/mnt/user/books', '-e', '/mnt/user/books/excluded/'])
        self.assertEqual(engine.exclusions, ['/mnt/disk*/books/excluded'])
        self.assertTrue(engine.excluded(Path('/mnt/disk2/books/excluded/nested/a.pdf')))
        self.assertFalse(engine.excluded(Path('/mnt/disk2/books/excluded-old/a.pdf')))

    def test_symlink_scope_is_rejected_without_removal(self):
        left = self.fixture.put(self.a, 'outside/a.pdf')
        (self.a / 'books').symlink_to(self.a / 'outside', target_is_directory=True)
        self.fixture.put(self.b, 'books/b.pdf')
        with self.assertRaises(m.SafetyError):
            self.engine(['/mnt/user/books'], real=True).run()
        self.assertTrue(left.exists())

    def test_torrent_mode_master_can_be_outside_input_and_have_custom_name(self):
        master = self.fixture.put(self.a, 'seeded/custom-release/original.pdf')
        extra_master = self.fixture.put(self.b, 'seeded/other-copy/another.pdf')
        target = self.fixture.put(self.b, 'books/Author/book.pdf')
        engine = self.engine(['--directory', '/mnt/user/books', '--master-dir', '/mnt/user/seeded'], real=True)
        result = engine.run()
        # Both masters match; B is roomier, so the B master wins. Its file is
        # on the target disk, hence no cross-disk action is needed for books.
        self.assertEqual(result.get('completed', 0), 0)
        self.assertTrue(master.exists())
        self.assertTrue(extra_master.exists())
        self.assertTrue(target.exists())

    def test_torrent_mode_retains_master_even_on_fuller_disk(self):
        master = self.fixture.put(self.a, 'seeded/original.pdf')
        old = self.fixture.put(self.b, 'books/Author/book.pdf')
        engine = self.engine(['--directory', '/mnt/user/books', '--master-dir', '/mnt/user/seeded'], real=True)
        result = engine.run()
        self.assertEqual(result['completed'], 1)
        self.assertFalse(old.exists())
        self.assertTrue(os.path.samefile(master, self.a / 'books/Author/book.pdf'))
        self.assertEqual(engine.mode, 'torrent')

    def test_unmatched_general_duplicates_are_skipped_in_torrent_mode(self):
        left, right = self.pair()
        self.fixture.put(self.a, 'seeded/different.pdf', b'different')
        result = self.engine(['--directory', '/mnt/user/books', '--master-dir', '/mnt/user/seeded'], real=True).run()
        self.assertEqual(result['duplicate_groups'], 0)
        self.assertTrue(left.exists())
        self.assertTrue(right.exists())

    def test_explicit_master_scope_does_not_accept_unrelated_torrent_component(self):
        left = self.fixture.put(self.a, 'books/torrents/a.pdf')
        right = self.fixture.put(self.b, 'books/b.pdf')
        self.fixture.put(self.a, 'seeded/different.pdf', b'different')
        result = self.engine(['--directory', '/mnt/user/books', '--master-dir', '/mnt/user/seeded'], real=True).run()
        self.assertEqual(result['duplicate_groups'], 0)
        self.assertTrue(left.exists())
        self.assertTrue(right.exists())

    def test_torrent_mode_without_master_keeps_legacy_component_detection(self):
        self.fixture.put(self.a, 'books/torrents/a.pdf')
        self.fixture.put(self.b, 'books/b.pdf')
        result = self.engine(['--mode', 'torrent', '/mnt/user/books']).run()
        self.assertEqual(result['planned'], 1)

    def test_master_missing_even_without_input_scope_is_an_error(self):
        self.pair()
        with self.assertRaises(m.SafetyError):
            self.engine(['--master-dir', '/mnt/user/missing']).run()

    def test_identical_relative_paths_in_master_and_target_scope_are_collapsed(self):
        self.fixture.put(self.a, 'books/torrents/a.pdf')
        self.fixture.put(self.b, 'books/b.pdf')
        engine = self.engine(['--directory', '/mnt/user/books', '--master-dir', '/mnt/user/books/torrents'])
        engine.scan()
        self.assertEqual(len(engine.inventory), 2)

    def test_initial_snapshot_is_used_for_all_groups(self):
        self.pair()
        self.fixture.pair('books/second.txt', 'books/renamed.txt', content=b'other')
        engine = self.engine(['/mnt/user/books'])
        result = engine.run()
        self.assertEqual(self.usage.call_count, 2)
        self.assertEqual(result['planned'], 2)
        for event, fields in engine.journal.events:
            if event == 'WOULD_DEDUPLICATE':
                self.assertTrue(fields['source'].startswith(str(self.b)))
        self.assertEqual(sum(event == 'DISK_USAGE_SNAPSHOT' for event, _ in engine.journal.events), 1)

    def test_free_space_ties_break_by_disk_number_then_path(self):
        left, right = self.pair()
        self.capacities[self.b.stat().st_dev] = (10000, 900)
        engine = self.engine(['/mnt/user/books'])
        engine.run()
        planned = [fields for event, fields in engine.journal.events if event == 'WOULD_DEDUPLICATE']
        self.assertEqual(planned[0]['source'], str(left))

    def test_space_poll_failure_aborts_before_deduplication(self):
        left, right = self.pair()
        engine = self.engine(['/mnt/user/books'], real=True)
        self.usage.side_effect = OSError('injected usage failure')
        with self.assertRaises(OSError):
            engine.run()
        self.assertTrue(left.exists())
        self.assertTrue(right.exists())

    def test_fuller_disk_duplicates_are_processed_first(self):
        left, right = self.pair()
        third = self.a.parent / 'disk3'
        third.mkdir()
        extra = self.fixture.put(third, 'books/third.pdf')
        engine = self.engine(['/mnt/user/books'])
        engine.roots[str(third)] = m.PhysicalDisk(third)
        engine.scan()
        entries = engine.inventory
        usage = {str(self.a): {'available_bytes': 10}, str(self.b): {'available_bytes': 30},
                 str(third): {'available_bytes': 20}}
        order = []
        def snapshot():
            engine.disk_usage = usage
        def scan():
            engine.inventory = entries
        with patch.object(engine, 'snapshot_disk_usage', side_effect=snapshot), patch.object(engine, 'scan', side_effect=scan):
            with patch.object(engine, 'groups', return_value=[entries]), patch.object(engine, 'deduplicate', side_effect=lambda s, d: order.append(d.path)):
                engine.run()
        self.assertEqual(order, [left, extra])

    def test_excluded_replacement_is_never_created(self):
        left, right = self.pair()
        engine = self.engine(['/mnt/user/books', '-e', str(self.b / 'books/Authors')], real=True)
        with self.assertRaises(m.SafetyError):
            engine.run()
        self.assertTrue(left.exists())
        self.assertTrue(right.exists())

    def test_argument_mode_defaults_and_invalid_combinations(self):
        self.assertEqual(m.arguments([]).mode, 'torrent')
        self.assertEqual(m.arguments(['/mnt/user/books']).mode, 'general')
        self.assertEqual(m.arguments(['--directory', '/mnt/user/books']).mode, 'general')
        self.assertEqual(m.arguments(['--master-dir', '/mnt/user/seeded']).mode, 'torrent')
        self.assertEqual(m.arguments(['--mode', 'general']).mode, 'general')
        mixed = m.arguments(['/mnt/user/books', '/mnt/disk1', '/mnt/disk2'])
        self.assertEqual(mixed.disks, ['/mnt/disk1', '/mnt/disk2'])
        self.assertEqual(mixed.directories, ['/mnt/user/books'])
        interleaved = m.arguments(['/mnt/user/books', '--dry-run', '/mnt/disk1', '/mnt/disk2'])
        self.assertEqual(interleaved.disks, mixed.disks)
        self.assertEqual(interleaved.directories, mixed.directories)
        for argv in (['--mode', 'general', '--master-dir', '/mnt/user/seeded'],
                     ['-p', '/tmp/files', '--directory', '/mnt/user/books']):
            with self.subTest(argv=argv), patch('sys.stderr'), self.assertRaises(SystemExit):
                m.arguments(argv)

    def test_invalid_scope_paths_are_rejected(self):
        for path in ('books', '/mnt/user0/books', '/mnt/cache/books', '/mnt/user/books/../other',
                     '/mnt/user/books//nested', '/mnt/user/books/./nested', '/mnt/user', '/mnt/disk99/books'):
            with self.subTest(path=path), self.assertRaises(m.SafetyError):
                m.directory_scope(path, [self.a, self.b])

    def test_general_file_list_mode_does_not_require_a_torrent_name(self):
        left, right = self.pair()
        listing = self.a.parent / 'file-list.txt'
        listing.write_text(str(left) + '\n' + str(right) + '\n')
        result = self.engine(['--mode', 'general', '--process-file', str(listing)]).run()
        self.assertEqual(result['planned'], 1)


if __name__ == '__main__':
    unittest.main()
