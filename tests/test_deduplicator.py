"""Regression tests use disposable /tmp and /dev/shm files, never /mnt."""
import argparse
import contextlib
import errno
import importlib.util
import io
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / 'unraid-cross-disk-deduplicator.py'
spec = importlib.util.spec_from_file_location('dedup_test_module', SCRIPT)
m = importlib.util.module_from_spec(spec)
sys.modules[m.__name__] = m
spec.loader.exec_module(m)
m.LOG.addHandler(logging.NullHandler())

class Policy:
    def check(self, share, disk):
        pass

class Journal:
    def __init__(self):
        self.events = []
    def write(self, event, **fields):
        self.events.append((event, fields))

class DeduplicatorTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='dedup-tests-')
        self.shm = tempfile.TemporaryDirectory(prefix='dedup-tests-', dir='/dev/shm')
        self.a = Path(self.tmp.name) / 'disk1'; self.a.mkdir()
        self.b = Path(self.shm.name) / 'disk2'; self.b.mkdir()
        self.assertNotEqual(self.a.stat().st_dev, self.b.stat().st_dev)
        self.engines = []
    def tearDown(self):
        for engine in self.engines:
            engine.close()
        self.tmp.cleanup(); self.shm.cleanup()
    def put(self, root, relative, content=b'content'):
        path = root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        return path
    def pair(self, source='data/torrents/release.mkv', duplicate='data/media/renamed.mkv', content=b'content'):
        return self.put(self.a, source, content), self.put(self.b, duplicate, content)
    def engine(self, real=False, exclude=(), process_file=None, null=False, allow=False, policy=None, normalize=True, owner=None):
        args = m.arguments([])
        args.real_run=real; args.exclude=list(exclude); args.process_file=process_file
        args.null=null; args.allow_metadata_change=allow; args.normalize_metadata=normalize
        e = m.Engine(args, [self.a,self.b], Journal(), policy or Policy(),
                     owner=(os.getuid(), os.getgid()) if owner is None else owner)
        self.engines.append(e)
        return e
    def planned(self, engine):
        engine.scan(); group=engine.groups()[0]
        return next(e for e in group if e.torrent), next(e for e in group if not e.torrent)
    def test_dry_run_renamed_files_no_media_changes(self):
        src, old=self.pair()
        e=self.engine(); result=e.run()
        self.assertEqual(result['planned'],1)
        self.assertTrue(old.exists()); self.assertFalse((self.a/'data/media').exists())
        self.assertIn('WOULD_DEDUPLICATE', [event for event, _ in e.journal.events])
    def test_real_run_preserves_logical_path_and_inode(self):
        src,old=self.pair(); e=self.engine(True); result=e.run()
        target=self.a/'data/media/renamed.mkv'
        self.assertFalse(old.exists()); self.assertTrue(os.path.samefile(src,target))
        self.assertEqual(result['completed'],1)
        self.assertEqual(e.journal.events[-1][0],'ORIGINAL_REMOVED')
        self.assertEqual(e.run().get('completed',0),0)  # no new operation on repeat
    def test_brackets_newline_tab_backslash_unit_separator_names(self):
        name='Movie [1080p]\\tab\tline\nseparator\x1f.mkv'
        src,old=self.pair(duplicate='data/media/'+name)
        e=self.engine(True);e.run()
        target=self.a/'data/media'/name
        self.assertTrue(os.path.samefile(src,target));self.assertFalse(old.exists())
    def test_undecodable_filename(self):
        name=os.fsdecode(b'movie_\xff.mkv')
        src,old=self.pair(duplicate='data/media/'+name)
        self.engine(True).run();self.assertTrue(os.path.samefile(src,self.a/'data/media'/name))
    def test_link_failure_preserves_original(self):
        src,old=self.pair(); e=self.engine(True)
        with patch.object(m.os,'link',side_effect=OSError(errno.EXDEV,'injected')):
            with self.assertRaises(m.SafetyError):e.run()
        self.assertTrue(old.exists()); self.assertTrue(src.exists())
        self.assertFalse((self.a/'data/media/renamed.mkv').exists())
    def test_existing_correct_link_finishes_partial_run(self):
        src,old=self.pair(); target=self.a/'data/media/renamed.mkv'
        target.parent.mkdir();os.link(src,target)
        e=self.engine(True);e.run()
        self.assertFalse(old.exists());self.assertTrue(os.path.samefile(src,target))
    def test_collision_keeps_original(self):
        src,old=self.pair(); collision=self.put(self.a,'data/media/renamed.mkv',b'different')
        with self.assertRaises(m.SafetyError):self.engine(True).run()
        self.assertEqual(collision.read_bytes(),b'different');self.assertTrue(old.exists())
    def test_dry_collision_detected(self):
        src,old=self.pair(); self.put(self.a,'data/media/renamed.mkv',b'different')
        with self.assertRaises(m.SafetyError):self.engine().run()
        self.assertTrue(old.exists())
    def test_changed_file_rejected(self):
        src,old=self.pair();e=self.engine(True);source,duplicate=self.planned(e)
        old.write_bytes(b'changed')
        with self.assertRaises(m.ChangedFile):e.deduplicate(source,duplicate)
        self.assertTrue(old.exists())
    def test_change_during_hash_rejected(self):
        src,old=self.pair();e=self.engine();e.scan(); original=m.os.read; changed=False
        def read(fd,n):
            nonlocal changed
            value=original(fd,n)
            if not changed:
                changed=True;src.write_bytes(b'changed-content')
            return value
        with patch.object(m.os,'read',side_effect=read):
            with self.assertRaises(m.SafetyError):e.groups()
        self.assertTrue(old.exists())
    def test_mutation_after_link_rolls_back_and_retains_original(self):
        src,old=self.pair();e=self.engine(True);actual=m.os.link
        def link(*args,**kwargs):
            actual(*args,**kwargs);src.write_bytes(b'changed')
        with patch.object(m.os,'link',side_effect=link):
            with self.assertRaises(m.SafetyError):e.run()
        self.assertTrue(old.exists());self.assertFalse((self.a/'data/media/renamed.mkv').exists())
    def test_sigint_after_link_retains_original(self):
        src,old=self.pair();e=self.engine(True);original=e.journal.write
        def write(event,**fields):
            if event=='LINK_READY':raise KeyboardInterrupt()
            original(event,**fields)
        e.journal.write=write
        with self.assertRaises(KeyboardInterrupt):e.run()
        self.assertTrue(old.exists());self.assertFalse((self.a/'data/media/renamed.mkv').exists())
    def test_signal_immediately_after_unlink_keeps_replacement(self):
        src,old=self.pair();e=self.engine(True);actual=m.os.unlink
        def unlink(name,*args,**kwargs):
            actual(name,*args,**kwargs)
            if name=='renamed.mkv':raise KeyboardInterrupt()
        with patch.object(m.os,'unlink',side_effect=unlink):
            with self.assertRaises(KeyboardInterrupt):e.run()
        self.assertFalse(old.exists());self.assertTrue(os.path.samefile(src,self.a/'data/media/renamed.mkv'))
    def test_unlink_failure_keeps_both(self):
        src,old=self.pair();e=self.engine(True)
        with patch.object(m.os,'unlink',side_effect=PermissionError('injected')):
            with self.assertRaises(m.SafetyError):e.run()
        self.assertTrue(old.exists());self.assertTrue(os.path.samefile(src,self.a/'data/media/renamed.mkv'))
    def test_journal_failure_after_unlink_keeps_replacement(self):
        src,old=self.pair();e=self.engine(True);actual=e.journal.write
        def write(event,**fields):
            if event=='ORIGINAL_REMOVED':raise OSError('injected')
            actual(event,**fields)
        e.journal.write=write
        with self.assertRaises(m.SafetyError):e.run()
        self.assertFalse(old.exists());self.assertTrue(os.path.samefile(src,self.a/'data/media/renamed.mkv'))
    def test_symlinked_target_parent_rejected(self):
        src,old=self.pair();outside=Path(self.tmp.name)/'outside';outside.mkdir()
        (self.a/'data/media').symlink_to(outside,target_is_directory=True)
        with self.assertRaises(m.SafetyError):self.engine(True).run()
        self.assertTrue(old.exists());self.assertEqual(list(outside.iterdir()),[])
    def test_dangling_destination_symlink_rejected(self):
        src,old=self.pair();target=self.a/'data/media/renamed.mkv';target.parent.mkdir()
        target.symlink_to('/nonexistent')
        with self.assertRaises(m.SafetyError):self.engine(True).run()
        self.assertTrue(old.exists());self.assertTrue(target.is_symlink())
    def test_symlink_source_skipped(self):
        src,old=self.pair();src.unlink();src.symlink_to(old)
        result=self.engine(True).run();self.assertEqual(result.get('completed',0),0);self.assertTrue(old.exists())
    def test_two_exclusions_work(self):
        self.pair();self.put(self.b,'data/excluded1/x');self.put(self.b,'data/excluded2/y')
        e=self.engine(exclude=[str(self.b/'data/excluded1'),str(self.b/'data/excluded2')]);e.scan()
        self.assertEqual(len(e.inventory),2)
    def test_excluded_input_paths_are_not_processed(self):
        src,old=self.pair();f=Path(self.tmp.name)/'list';f.write_text(str(src)+'\n'+str(old))
        e=self.engine(process_file=str(f),exclude=[str(old.parent)]);e.scan()
        self.assertEqual(len(e.inventory),1)
    def test_nul_input_handles_newline(self):
        src,old=self.pair(duplicate='data/media/new\nline')
        f=Path(self.tmp.name)/'list';f.write_bytes(os.fsencode(src)+b'\0'+os.fsencode(old)+b'\0')
        self.assertEqual(self.engine(process_file=str(f),null=True).run()['planned'],1)
    def test_unterminated_nul_list_rejected(self):
        src,old=self.pair();f=Path(self.tmp.name)/'list';f.write_bytes(os.fsencode(src))
        with self.assertRaises(m.SafetyError):self.engine(process_file=str(f),null=True).run()
    def test_outside_input_root_rejected(self):
        f=Path(self.tmp.name)/'list';f.write_text('/etc/passwd\n')
        with self.assertRaises(m.SafetyError):self.engine(process_file=str(f)).run()
    def test_scan_error_blocks_all_mutations(self):
        src,old=self.pair();e=self.engine(True);actual=e.walk
        def walk(disk,rel):
            if disk.path==self.b:e.error('injected',OSError('injected'))
            else:actual(disk,rel)
        e.walk=walk
        with self.assertRaises(m.SafetyError):e.run()
        self.assertTrue(old.exists());self.assertFalse((self.a/'data/media').exists())
    def test_metadata_mismatch_skipped(self):
        src,old=self.pair();old.chmod(0o600);src.chmod(0o644)
        result=self.engine(True,normalize=False).run();self.assertEqual(result['metadata_conflicts_skipped'],1);self.assertTrue(old.exists())
    def test_explicit_metadata_override(self):
        src,old=self.pair();old.chmod(0o600);src.chmod(0o644)
        self.engine(True,allow=True,normalize=False).run();self.assertFalse(old.exists())
        self.assertEqual((self.a/'data/media/renamed.mkv').stat().st_mode & 0o777,0o644)
    def test_file_xattr_mismatch_skipped(self):
        src,old=self.pair();os.setxattr(src,'user.test',b'torrent')
        result=self.engine(True).run()
        self.assertEqual(result['metadata_conflicts_skipped'],1);self.assertTrue(old.exists())
    def test_directory_xattrs_preserved(self):
        src,old=self.pair();os.setxattr(old.parent,'user.test',b'directory')
        self.engine(True).run()
        self.assertEqual(os.getxattr(self.a/'data/media','user.test'),b'directory')
    def test_source_replaced_during_link_original_retained(self):
        src,old=self.pair();e=self.engine(True);actual=m.os.link
        def link(*args,**kwargs):
            src.unlink();src.symlink_to(old)
            return actual(*args,**kwargs)
        with patch.object(m.os,'link',side_effect=link):
            with self.assertRaises(m.SafetyError):e.run()
        self.assertTrue(old.exists());self.assertFalse((self.a/'data/media/renamed.mkv').exists())
    def test_global_disk_exclusion_rejected(self):
        config=Path(self.tmp.name)/'config';config.mkdir()
        (config/'share.cfg').write_text('shareUserExclude="disk1"\n')
        src,old=self.pair()
        with self.assertRaises(m.SafetyError):self.engine(True,policy=m.SharePolicy(config)).run()
        self.assertTrue(old.exists())
    def test_sync_failure_before_removal_preserves_original(self):
        src,old=self.pair();e=self.engine(True)
        # Existing destination directories avoid unrelated mkdir sync calls.
        (self.a/'data/media').mkdir()
        with patch.object(m.os,'fsync',side_effect=OSError('injected sync failure')):
            with self.assertRaises(m.SafetyError):e.run()
        self.assertTrue(old.exists())
    def test_directory_mode_preserved(self):
        src,old=self.pair();old.parent.chmod(0o750)
        self.engine(True,normalize=False).run();self.assertEqual((self.a/'data/media').stat().st_mode & 0o777,0o750)
    def test_other_duplicate_hardlinks_refreshed(self):
        src,old=self.pair();alias=self.b/'data/media/another.mkv';os.link(old,alias)
        result=self.engine(True).run()
        self.assertEqual(result['completed'],2);self.assertFalse(old.exists());self.assertFalse(alias.exists())
        self.assertTrue(os.path.samefile(src,self.a/'data/media/another.mkv'))
    def test_source_hardlink_aliases_refreshed(self):
        src,old=self.pair();alias=self.a/'data/torrents/alias.mkv';os.link(src,alias)
        old2=self.put(self.b,'data/media/second.mkv')
        result=self.engine(True).run();self.assertEqual(result['completed'],2)
        self.assertTrue(os.path.samefile(src,self.a/'data/media/second.mkv'))
    def test_multiple_copies_refresh_source(self):
        src,old=self.pair();self.put(self.b,'data/media/second.mkv')
        result=self.engine(True).run();self.assertEqual(result['completed'],2)
    def test_no_torrent_priority_no_action(self):
        self.put(self.a,'data/media/a');old=self.put(self.b,'data/media/b')
        self.assertEqual(self.engine(True).run().get('completed',0),0);self.assertTrue(old.exists())
    def test_same_size_different_content_retained(self):
        src,old=self.pair();old.write_bytes(b'another')
        self.assertEqual(self.engine(True).run().get('completed',0),0);self.assertTrue(old.exists())
    def test_only_same_disk_duplicates_untouched(self):
        self.put(self.a,'data/torrents/a');old=self.put(self.a,'data/media/b')
        self.assertEqual(self.engine(True).run().get('completed',0),0);self.assertTrue(old.exists())
    def test_empty_files_ignored(self):
        self.pair(content=b'');self.assertEqual(self.engine(True).run()['files_scanned'],0)
    def test_deterministic_priority(self):
        self.put(self.a,'data/torrents/z');first=self.put(self.a,'data/torrents/a');self.put(self.b,'data/media/b')
        self.engine(True).run();self.assertTrue(os.path.samefile(first,self.a/'data/media/b'))
    def test_input_duplicates_deduplicated(self):
        src,old=self.pair();f=Path(self.tmp.name)/'list';f.write_text('\n'.join([str(src),str(old),str(old)]))
        self.assertEqual(self.engine(True,process_file=str(f)).run()['completed'],1)
    def test_unmounted_or_nonstandard_roots_refused(self):
        for roots in ([str(self.a),str(self.b)],['/mnt/user','/mnt/disk1'],['/mnt/disk1/../disk2']):
            with self.assertRaises(m.SafetyError):m.validate_roots(roots)
    def test_parser_short_and_long_flags(self):
        self.assertTrue(m.arguments(['--real-run']).real_run)
        self.assertTrue(m.arguments(['--dry-run']).dry_run)
        self.assertEqual(m.arguments(['-v','debug']).verbose,'DEBUG')
    def test_missing_share_config_blocks_mutation(self):
        src,old=self.pair();policy=m.SharePolicy(Path(self.tmp.name)/'missing')
        with self.assertRaises(m.SafetyError):self.engine(True,policy=policy).run()
        self.assertTrue(old.exists())
    def test_share_excluded_destination(self):
        config=Path(self.tmp.name)/'config';(config/'shares').mkdir(parents=True)
        (config/'share.cfg').write_text('shareUserInclude=""\n')
        (config/'shares/data.cfg').write_text('shareExclude="disk1"\n')
        src,old=self.pair()
        with self.assertRaises(m.SafetyError):self.engine(True,policy=m.SharePolicy(config)).run()
        self.assertTrue(old.exists())
    def test_pool_only_share_rejected(self):
        config=Path(self.tmp.name)/'config';(config/'shares').mkdir(parents=True)
        (config/'share.cfg').write_text('shareUserInclude=""\n')
        (config/'shares/data.cfg').write_text('sharePrimaryStorage="cache"\nshareSecondaryStorage=""\n')
        src,old=self.pair()
        with self.assertRaises(m.SafetyError):self.engine(True,policy=m.SharePolicy(config)).run()
        self.assertTrue(old.exists())
    def test_pool_only_legacy_config_rejected(self):
        config=Path(self.tmp.name)/'config';(config/'shares').mkdir(parents=True)
        (config/'share.cfg').write_text('shareUserInclude=""\n')
        (config/'shares/data.cfg').write_text('shareUseCache="only"\nshareCachePool="cache"\n')
        src,old=self.pair()
        with self.assertRaises(m.SafetyError):self.engine(True,policy=m.SharePolicy(config)).run()
        self.assertTrue(old.exists())
    def test_json_journal_escapes_filename_and_roundtrips(self):
        path=Path(self.tmp.name)/'journal';j=m.Journal(path)
        value='tab\tnewline\nback\\'+os.fsdecode(b'\xff');j.write('TEST',path=value);j.close()
        self.assertEqual(len(path.read_text().splitlines()),1)
        self.assertEqual(json.loads(path.read_text())['path'],value)
    def test_normalization_enabled_by_default(self):
        self.assertTrue(m.arguments([]).normalize_metadata)
        self.assertFalse(m.arguments(['--no-normalize-metadata']).normalize_metadata)
    def test_file_mode_policy_is_explicit_666(self):
        path = self.put(self.a, 'data/example')
        for mode in (0o000, 0o200, 0o400, 0o600, 0o644, 0o660, 0o755, 0o777):
            path.chmod(mode)
            self.assertEqual(m.normalized_mode(path.stat()), 0o666)
    def test_default_repairs_file_modes_and_keeps_torrent_inode(self):
        src,old=self.pair();src.chmod(0o644);old.chmod(0o600)
        inode=src.stat().st_ino
        result=self.engine(True).run()
        target=self.a/'data/media/renamed.mkv'
        self.assertFalse(old.exists());self.assertEqual(target.stat().st_ino,inode)
        self.assertEqual(target.stat().st_mode & 0o777,0o666)
        self.assertTrue(os.path.samefile(src,target))
        self.assertEqual(result['completed'],1)
    def test_default_sets_involved_directories_to_777(self):
        src,old=self.pair();src.parent.chmod(0o700);old.parent.chmod(0o750)
        self.engine(True).run()
        for path in (self.a/'data',src.parent,self.b/'data',old.parent,self.a/'data/media'):
            self.assertEqual(path.stat().st_mode & 0o777,0o777)
    def test_normalization_dry_run_preserves_modes_owners_and_timestamps(self):
        src,old=self.pair();src.chmod(0o644);old.chmod(0o600)
        before=[m.stamp(path.stat()) for path in (src,old)]
        result=self.engine().run()
        self.assertEqual(before,[m.stamp(path.stat()) for path in (src,old)])
        self.assertGreater(result['metadata_normalizations_planned'],0)
        self.assertFalse((self.a/'data/media').exists())
    def test_torrent_readonly_mode_is_normalized_to_666(self):
        src,old=self.pair();src.chmod(0o400);old.chmod(0o600)
        self.engine(True).run()
        self.assertEqual((self.a/'data/media/renamed.mkv').stat().st_mode & 0o777,0o666)
        self.assertTrue(os.path.samefile(src,self.a/'data/media/renamed.mkv'))
    def test_normalization_strips_execute_bits(self):
        src,old=self.pair();src.chmod(0o755);old.chmod(0o700)
        self.engine(True).run()
        self.assertEqual(src.stat().st_mode & 0o777,0o666)
    def test_link_failure_retains_repaired_original(self):
        src,old=self.pair();src.chmod(0o644);old.chmod(0o600)
        with patch.object(m.os,'link',side_effect=OSError('injected')):
            with self.assertRaises(m.SafetyError):self.engine(True).run()
        self.assertTrue(old.exists());self.assertEqual(old.stat().st_mode & 0o777,0o666)
    def test_chown_failure_aborts_without_removal(self):
        src,old=self.pair()
        with patch.object(m.os,'fchown',side_effect=PermissionError('injected')) as change:
            with self.assertRaises(m.SafetyError):self.engine(True,owner=(os.getuid()+1,os.getgid())).run()
        self.assertTrue(change.called);self.assertTrue(old.exists())
        self.assertFalse((self.a/'data/media/renamed.mkv').exists())
    def test_contents_changed_during_normalization_rejected(self):
        src,old=self.pair();actual=m.os.fchmod
        def change(fd,mode):
            actual(fd,mode)
            if m.identity(os.fstat(fd))==m.identity(src.stat()):
                src.write_bytes(b'changed data')
        with patch.object(m.os,'fchmod',side_effect=change):
            with self.assertRaises(m.SafetyError):self.engine(True).run()
        self.assertTrue(old.exists());self.assertEqual(old.read_bytes(),b'content')
    def test_account_lookup_uses_nobody_users(self):
        from types import SimpleNamespace
        with patch.object(m.pwd,'getpwnam',return_value=SimpleNamespace(pw_uid=99)) as user:
            with patch.object(m.grp,'getgrnam',return_value=SimpleNamespace(gr_gid=100)) as group:
                self.assertEqual(m.unraid_owner(),(99,100))
        user.assert_called_once_with('nobody');group.assert_called_once_with('users')
    def test_matching_acl_after_chmod_is_planned_and_processed(self):
        src,old=self.pair()
        def acl(mask,other):
            entries=[(1,6,0xffffffff),(2,4,os.getuid()),(4,4,0xffffffff),
                     (16,mask,0xffffffff),(32,other,0xffffffff)]
            return m.struct.pack('<I',2)+b''.join(m.struct.pack('<HHI',*entry) for entry in entries)
        try:
            os.setxattr(src,'system.posix_acl_access',acl(4,4))
        except OSError as exc:
            if exc.errno in (errno.ENOTSUP,errno.EOPNOTSUPP):
                self.skipTest('Test filesystem does not support POSIX access ACLs')
            raise
        os.setxattr(old,'system.posix_acl_access',acl(0,0))
        self.assertEqual(self.engine().run()['planned'],1)
        self.assertEqual(self.engine(True).run()['completed'],1)
        self.assertEqual(src.stat().st_mode & 0o777,0o666)
    def test_acl_preview_updates_only_chmod_controlled_entries(self):
        entries=[(1,6,0xffffffff),(2,4,os.getuid()),(4,4,0xffffffff),
                 (16,0,0xffffffff),(32,0,0xffffffff)]
        raw=m.struct.pack('<I',2)+b''.join(m.struct.pack('<HHI',*entry) for entry in entries)
        result=m.projected_xattrs({'system.posix_acl_access':raw,'user.keep':b'value'},0o666)
        self.assertEqual(result['user.keep'],b'value')
        self.assertEqual(m.struct.unpack_from('<HHI',result['system.posix_acl_access'],12),entries[1])
        self.assertEqual(m.struct.unpack_from('<HHI',result['system.posix_acl_access'],20),entries[2])
        self.assertEqual(m.struct.unpack_from('<HHI',result['system.posix_acl_access'],28)[1],6)
        self.assertEqual(m.struct.unpack_from('<HHI',result['system.posix_acl_access'],36)[1],6)
    def test_unmatched_content_metadata_not_changed(self):
        src,old=self.pair();old.write_bytes(b'another');src.chmod(0o600);old.chmod(0o640)
        self.engine(True).run()
        self.assertEqual(src.stat().st_mode & 0o777,0o600)
        self.assertEqual(old.stat().st_mode & 0o777,0o640)
    def test_dryrun_chown_uses_requested_ids_without_mutation(self):
        src,old=self.pair();e=self.engine(owner=(99,100))
        with patch.object(m.os,'fchown') as change:e.run()
        change.assert_not_called()
        plans=[fields for event,fields in e.journal.events if event=='WOULD_NORMALIZE_METADATA']
        self.assertTrue(plans);self.assertTrue(all(item['after']['uid']==99 and item['after']['gid']==100 for item in plans))

    def test_directory_repair_from_666_assigns_777_directly(self):
        directory = self.a / 'data'
        directory.mkdir()
        fd = os.open(directory, m.DIR_FLAGS)
        engine = self.engine(True)
        actual_chmod = m.os.fchmod
        observed_modes = []
        try:
            directory.chmod(0o666)
            def checked_chmod(handle, mode):
                if m.stat.S_ISDIR(os.fstat(handle).st_mode):
                    observed_modes.append(mode)
                    self.assertEqual(mode, 0o777)
                actual_chmod(handle, mode)
            with patch.object(m.os, 'fchmod', side_effect=checked_chmod):
                engine.normalize_fd(fd, directory, 'repair-test')
            self.assertEqual(observed_modes, [0o777])
            self.assertEqual(directory.stat().st_mode & 0o777, 0o777)
        finally:
            os.close(fd)
            directory.chmod(0o777)
    def test_dedup_never_applies_file_mode_to_directory(self):
        self.pair()
        actual_chmod = m.os.fchmod
        directory_changes = []
        file_changes = []
        def checked_chmod(fd, mode):
            if m.stat.S_ISDIR(os.fstat(fd).st_mode):
                directory_changes.append(mode)
                self.assertTrue(mode & 0o111)
                self.assertNotEqual(mode, 0o666)
            else:
                file_changes.append(mode)
                self.assertEqual(mode, 0o666)
            actual_chmod(fd, mode)
        with patch.object(m.os, 'fchmod', side_effect=checked_chmod):
            self.engine(True).run()
        self.assertTrue(directory_changes)
        self.assertTrue(file_changes)
    def test_interruption_during_repair_keeps_directory_execute_bits(self):
        source, original = self.pair()
        source.parent.chmod(0o700)
        original.parent.chmod(0o700)
        actual_chmod = m.os.fchmod
        def interrupted_chmod(fd, mode):
            actual_chmod(fd, mode)
            if m.stat.S_ISDIR(os.fstat(fd).st_mode):
                self.assertEqual(os.fstat(fd).st_mode & 0o777, 0o777)
                raise KeyboardInterrupt()
        with patch.object(m.os, 'fchmod', side_effect=interrupted_chmod):
            with self.assertRaises(KeyboardInterrupt):
                self.engine(True).run()
        self.assertTrue(source.exists())
        self.assertTrue(original.exists())
        for directory in (self.a / 'data', source.parent, self.b / 'data', original.parent):
            self.assertTrue(directory.stat().st_mode & 0o111)
    def test_directory_metadata_copy_rejects_regular_file_source(self):
        file = self.put(self.a, 'data/file')
        destination = self.a / 'data/directory'
        destination.mkdir(mode=0o700)
        file_fd = os.open(file, m.FILE_FLAGS)
        directory_fd = os.open(destination, m.DIR_FLAGS)
        before = destination.stat().st_mode
        try:
            with self.assertRaises(m.SafetyError):
                m.copy_directory_metadata(file_fd, directory_fd)
            self.assertEqual(destination.stat().st_mode, before)
        finally:
            os.close(file_fd)
            os.close(directory_fd)
    def test_directory_metadata_copy_rejects_nontraversable_mode(self):
        source = self.a / 'data'
        destination = self.a / 'destination'
        source.mkdir(mode=0o700)
        destination.mkdir(mode=0o700)
        source_fd = os.open(source, m.DIR_FLAGS)
        destination_fd = os.open(destination, m.DIR_FLAGS)
        try:
            source.chmod(0o666)
            with self.assertRaises(m.SafetyError):
                m.copy_directory_metadata(source_fd, destination_fd)
            self.assertEqual(destination.stat().st_mode & 0o777, 0o700)
        finally:
            source.chmod(0o700)
            os.close(source_fd)
            os.close(destination_fd)
    def test_permission_policy_rejects_special_files(self):
        fifo = self.a / 'fifo'
        os.mkfifo(fifo)
        with self.assertRaises(m.SafetyError):
            m.normalized_mode(fifo.stat())

    def test_help_and_version_from_python_entry(self):
        result=subprocess.run([sys.executable,str(SCRIPT),'--help'],capture_output=True,text=True)
        self.assertEqual(result.returncode,0);self.assertIn('--real-run',result.stdout)
        result=subprocess.run([sys.executable,str(SCRIPT),'--version'],capture_output=True,text=True)
        self.assertEqual(result.stdout.strip(),m.VERSION)

if __name__=='__main__':unittest.main(verbosity=2)
