#!/usr/bin/env python3
"""Event-driven public digest mirror. No credentials, SSH, polling, or server changes."""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import socket
import sys
import time
import urllib.error
import urllib.request

SOURCE = 'https://raw.githubusercontent.com/Ideal-ljl/Ideal-ljl/arxiv-daily-data'
TARGET = 'http://43.199.158.51:18081'
MAX_BYTES = 2 * 1024 * 1024
ATTEMPTS = 3
BACKOFF = (5, 15)
POST_INTERVAL = 6.0  # Less than the server's global 12 pushes/minute limit.


class SyncError(RuntimeError):
    pass


class RequestError(SyncError):
    def __init__(self, message, status=None, retryable=True):
        super().__init__(message)
        self.status = status
        self.retryable = retryable


def require(condition, message):
    if not condition:
        raise SyncError(message)


def pairs_unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('Duplicate JSON key')
        result[key] = value
    return result


def decode(raw):
    def invalid_constant(value):
        raise ValueError('Non-finite JSON number')
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs_unique,
                      parse_constant=invalid_constant)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(',', ':'), allow_nan=False).encode('utf-8')


def digest_hash(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def valid_date(value):
    require(isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', value), 'Invalid date')
    try:
        dt.date.fromisoformat(value)
    except ValueError as exc:
        raise SyncError('Invalid date') from exc
    return value


def checked_at(value):
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        require(parsed.utcoffset() is not None, 'checkedAt must include a timezone')
        return parsed
    except (AttributeError, ValueError) as exc:
        raise SyncError('Invalid checkedAt') from exc


def validate_digest(value, day):
    require(isinstance(value, dict) and value.get('date') == day, 'Digest date mismatch')
    papers = value.get('papers')
    counts = value.get('counts')
    require(isinstance(papers, list) and isinstance(counts, dict), 'Invalid digest structure')
    require(type(counts.get('selected')) is int and counts['selected'] == len(papers), 'Selected count mismatch')
    ids = [paper.get('id') for paper in papers if isinstance(paper, dict)]
    require(len(ids) == len(papers) and all(isinstance(x, str) for x in ids)
            and len(set(ids)) == len(ids), 'Invalid or duplicate paper IDs')
    ranks = [p.get('topRank') for p in papers if p.get('topRank') is not None]
    require(all(type(rank) is int for rank in ranks) and len(set(ranks)) == len(ranks), 'Invalid Top3 ranks')
    require(sorted(ranks) == list(range(1, min(3, len(papers)) + 1)), 'Top3 ranks incomplete')
    checked_at(value.get('checkedAt'))
    return value


def read_regular_json(path):
    require(not path.is_symlink() and path.is_file(), f'Expected regular file: {path}')
    require(0 < path.stat().st_size <= MAX_BYTES, f'Invalid file size: {path}')
    try:
        return decode(path.read_bytes())
    except (ValueError, UnicodeError) as exc:
        raise SyncError(f'Invalid JSON: {path}') from exc


def load_source(root):
    archive = read_regular_json(root / 'archive.json')
    require(isinstance(archive, dict) and isinstance(archive.get('archive'), list), 'Invalid source archive')
    dates = []
    result = {}
    for meta in archive['archive']:
        require(isinstance(meta, dict), 'Invalid source metadata')
        day = valid_date(meta.get('date'))
        require(day not in result, 'Duplicate source date')
        envelope = read_regular_json(root / 'digests' / (day + '.json'))
        require(isinstance(envelope, dict) and set(envelope) == {'digest'}, 'Invalid source envelope')
        digest = validate_digest(envelope['digest'], day)
        require(meta.get('status') == digest.get('status') and meta.get('sourceBatch') == digest.get('sourceBatch')
                and type(meta.get('count')) is int and meta['count'] == len(digest['papers']), 'Source metadata mismatch')
        require(isinstance(meta.get('updatedAt'), str), 'Missing source updatedAt')
        result[day] = digest
        dates.append(day)
    require(dates and dates == sorted(dates, reverse=True), 'Source archive must be nonempty and date-descending')
    return result


def archive_map(value):
    require(isinstance(value, dict) and isinstance(value.get('digests'), list), 'Invalid target archive')
    result = {}
    for row in value['digests']:
        require(isinstance(row, dict), 'Invalid target archive row')
        day = valid_date(row.get('date'))
        require(day not in result, 'Duplicate target date')
        require(type(row.get('revision')) is int and row['revision'] > 0, 'Invalid target revision')
        require(type(row.get('selected')) is int and row['selected'] >= 0, 'Invalid target count')
        require(isinstance(row.get('sha256'), str) and re.fullmatch('[0-9a-f]{64}', row['sha256']), 'Invalid target hash')
        checked_at(row.get('checkedAt'))
        result[day] = row
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class Transport:
    """The environment's normal proxy settings and HTTPS verification stay enabled."""
    def __init__(self):
        self.opener = urllib.request.build_opener(NoRedirect())

    def request(self, url, payload=None):
        allowed = (url == SOURCE + '/archive.json'
                   or re.fullmatch(re.escape(SOURCE) + r'/digests/\d{4}-\d{2}-\d{2}\.json', url)
                   or url == TARGET + '/api/archive'
                   or re.fullmatch(re.escape(TARGET) + r'/api/digests\?date=\d{4}-\d{2}-\d{2}(?:&revision=[1-9][0-9]{0,8})?', url)
                   or url == TARGET + '/api/push')
        require(allowed and (payload is None or url == TARGET + '/api/push'), 'Unapproved request target')
        headers = {'Accept': 'application/json', 'Cache-Control': 'no-cache',
                   'User-Agent': 'arxiv-daily-github-sync/1.0', 'Connection': 'close'}
        if payload is not None:
            headers['Content-Type'] = 'application/json'
        request = urllib.request.Request(url, data=payload, headers=headers)
        try:
            with self.opener.open(request, timeout=30) as response:
                if response.status != 200:
                    raise RequestError(f'Unexpected HTTP {response.status}', response.status)
                length = response.headers.get('Content-Length')
                if length is not None:
                    if not length.isdigit() or int(length) > MAX_BYTES:
                        raise RequestError('Invalid or oversized Content-Length', retryable=False)
                body = response.read(MAX_BYTES + 1)
                if len(body) > MAX_BYTES:
                    raise RequestError('Response exceeds size limit', retryable=False)
                if length is not None and len(body) != int(length):
                    raise RequestError(f'Truncated response: {len(body)}/{length} bytes')
                content_type = response.headers.get_content_type()
                if content_type not in ('application/json', 'text/plain'):
                    raise RequestError('Unexpected response content type')
                try:
                    return decode(body)
                except (ValueError, UnicodeError, RecursionError) as exc:
                    raise RequestError('Invalid response JSON') from exc
        except urllib.error.HTTPError as exc:
            retryable = exc.code in (408, 429, 500, 502, 503, 504)
            raise RequestError(f'HTTP {exc.code} from {url}', exc.code, retryable) from exc
        except (urllib.error.URLError, OSError, http.client.HTTPException) as exc:
            raise RequestError(f'Transport failure for {url}: {type(exc).__name__}') from exc


class Syncer:
    def __init__(self, transport, evidence, sleep=time.sleep, monotonic=time.monotonic):
        self.transport = transport
        self.evidence = evidence
        self.evidence.mkdir(parents=True, exist_ok=True)
        self.sleep = sleep
        self.monotonic = monotonic
        self.last_post = None

    def save(self, name, value):
        (self.evidence / name).write_bytes(canonical(value) + b'\n')

    def get(self, url, missing_ok=False):
        for attempt in range(ATTEMPTS):
            try:
                return self.transport.request(url)
            except RequestError as exc:
                if exc.status == 404 and missing_ok:
                    return None
                if not exc.retryable or attempt == ATTEMPTS - 1:
                    raise
                print(f'Retrying read after {exc}; attempt {attempt + 2}/{ATTEMPTS}', flush=True)
                self.sleep(BACKOFF[attempt])
        raise SyncError('Unreachable retry state')

    def target_archive(self):
        return archive_map(self.get(TARGET + '/api/archive'))

    def readback(self, day, missing_ok=False, revision=None):
        suffix = '' if revision is None else '&revision=' + str(revision)
        return self.get(TARGET + '/api/digests?date=' + day + suffix, missing_ok)

    def ensure_live_source(self, expected):
        day = expected['date']
        for attempt in range(ATTEMPTS):
            live = self.get(SOURCE + '/digests/' + day + '.json')
            if isinstance(live, dict) and canonical(live.get('digest')) == canonical(expected):
                return
            if attempt < ATTEMPTS - 1:
                print(f'{day}: waiting for fixed raw source consistency', flush=True)
                self.sleep(BACKOFF[attempt])
        raise SyncError(f'{day}: fixed live source differs from checkout; use the latest workflow run')

    def post(self, expected, require_unchanged=False):
        """Never automatically replay an uncertain POST without a successful reconciliation GET."""
        day = expected['date']
        sha = digest_hash(expected)
        for attempt in range(ATTEMPTS):
            if self.last_post is not None:
                self.sleep(max(0, POST_INTERVAL - (self.monotonic() - self.last_post)))
            self.last_post = self.monotonic()
            try:
                result = self.transport.request(TARGET + '/api/push', canonical(expected))
            except RequestError as exc:
                if not exc.retryable and exc.status != 409:
                    raise
                # A successful write may have lost its response. Read before any repeat.
                actual = self.readback(day, missing_ok=True)
                if actual is not None and canonical(actual) == canonical(expected) and not require_unchanged:
                    print(f'{day}: uncertain POST reconciled by full JSON readback', flush=True)
                    return None
                if actual is not None and canonical(actual) != canonical(expected):
                    require(checked_at(actual.get('checkedAt')) < checked_at(expected['checkedAt']),
                            f'{day}: server already has a newer or conflicting revision')
                if attempt == ATTEMPTS - 1:
                    raise SyncError(f'{day}: POST confirmation unavailable after reconciliation') from exc
                print(f'{day}: POST uncertain; readback completed before retry {attempt + 2}/{ATTEMPTS}', flush=True)
                self.sleep(BACKOFF[attempt])
                self.ensure_live_source(expected)
                continue
            require(isinstance(result, dict) and result.get('date') == day and result.get('sha256') == sha,
                    f'{day}: invalid POST receipt')
            require(type(result.get('revision')) is int and result['revision'] > 0, 'Invalid POST revision')
            require(result.get('result') in ('created', 'updated', 'unchanged'), 'Invalid POST result')
            require(result.get('verifiedSource') == SOURCE and canonical(result.get('counts')) == canonical(expected['counts']),
                    'Server did not confirm fixed source and counts')
            if require_unchanged:
                require(result['result'] == 'unchanged', f'{day}: repeat POST was not unchanged')
            return result
        raise SyncError('Unreachable POST retry state')

    def verify_date(self, expected):
        day = expected['date']
        actual = self.readback(day)
        require(canonical(actual) == canonical(expected), f'{day}: full JSON readback mismatch')
        meta = self.target_archive().get(day)
        require(meta is not None and meta['sha256'] == digest_hash(expected)
                and meta['selected'] == len(expected['papers'])
                and meta.get('status') == expected['status']
                and meta.get('checkedAt') == expected['checkedAt'], f'{day}: archive metadata mismatch')
        return actual, meta

    def run(self, source):
        baseline = self.target_archive()  # No POST before a valid, complete baseline.
        self.save('archive-before.json', {'digests': list(baseline.values())})
        changed = [day for day in sorted(source)
                   if day not in baseline or baseline[day]['sha256'] != digest_hash(source[day])]
        print(f'Baseline: {len(baseline)} target dates; {len(source)} source dates; {len(changed)} to sync', flush=True)
        report = {'status': 'verified', 'source': SOURCE, 'target': TARGET,
                  'sourceDates': len(source), 'baselineDates': len(baseline),
                  'changedDates': changed, 'verified': [], 'unchangedHashDates': sorted(set(source) - set(changed)),
                  'eventCommit': os.environ.get('GITHUB_SHA'), 'sourceCommit': os.environ.get('SYNC_SOURCE_COMMIT')}
        prior_revisions = {}
        for day in changed:
            expected = source[day]
            if day in baseline:
                require(checked_at(expected['checkedAt']) > checked_at(baseline[day]['checkedAt']),
                        f'{day}: refusing stale or same-timestamp revision')
                old = self.readback(day, revision=baseline[day]['revision'])
                require(digest_hash(old) == baseline[day]['sha256'], f'{day}: prior revision hash mismatch')
                prior_revisions[day] = old
                self.save(day + '-prior-revision.json', old)
            self.ensure_live_source(expected)
            self.save(day + '-expected.json', expected)
            first = self.post(expected)
            self.save(day + '-first-receipt.json', first)
            actual, meta = self.verify_date(expected)
            if first is not None:
                require(first['revision'] == meta['revision'], f'{day}: receipt/revision mismatch')
            revision_before_repeat = meta['revision']
            second = self.post(expected, require_unchanged=True)
            self.save(day + '-repeat-receipt.json', second)
            actual, meta_after = self.verify_date(expected)
            require(meta_after['revision'] == second['revision'] == revision_before_repeat,
                    f'{day}: repeat push created another revision')
            require(canonical(meta_after) == canonical(meta), f'{day}: repeat push changed archive metadata')
            self.save(day + '-readback.json', actual)
            item = {'date': day, 'sha256': digest_hash(actual), 'revision': meta_after['revision'],
                    'count': len(actual['papers']), 'counts': actual['counts'],
                    'ids': [p['id'] for p in actual['papers']],
                    'top3': [{'id': p['id'], 'topRank': p['topRank']} for p in actual['papers'] if p.get('topRank') is not None],
                    'fullJsonExact': True, 'idempotent': True}
            report['verified'].append(item)
            print('DATE_VERIFIED ' + json.dumps(item, ensure_ascii=False, separators=(',', ':')), flush=True)
        # Even a no-change run performs an actual full readback of the latest date.
        latest = max(source)
        self.ensure_live_source(source[latest])
        latest_actual, _ = self.verify_date(source[latest])
        report['latestFullReadback'] = {'date': latest, 'sha256': digest_hash(latest_actual),
                                      'count': len(latest_actual['papers']), 'fullJsonExact': True}
        final = self.target_archive()
        require(set(final) == set(baseline) | set(source), 'Unexpected archive date addition or removal')
        for day, row in baseline.items():
            if day not in changed:
                require(canonical(final[day]) == canonical(row), f'{day}: unrelated historical metadata changed')
        require(all(final[day]['sha256'] == digest_hash(value) for day, value in source.items()),
                'Final archive does not match all source hashes')
        for day, old in prior_revisions.items():
            preserved = self.readback(day, revision=baseline[day]['revision'])
            require(canonical(preserved) == canonical(old), f'{day}: historical revision changed or disappeared')
        report['priorRevisionsPreserved'] = len(prior_revisions)
        report['finalArchive'] = [{'date': day, 'sha256': final[day]['sha256'],
                                   'revision': final[day]['revision'], 'count': final[day]['selected']}
                                  for day in sorted(final, reverse=True)]
        report.update(archiveDates=len(final), historicalDatesPreserved=len(set(baseline) - set(changed)),
                      historyPreserved=True)
        self.save('archive-after.json', {'digests': list(final.values())})
        self.save('verification.json', report)
        print('SYNC_VERIFICATION_JSON ' + json.dumps(report, ensure_ascii=False, separators=(',', ':')), flush=True)
        return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path('.'))
    parser.add_argument('--evidence-dir', type=Path, default=Path('sync-evidence'))
    args = parser.parse_args()
    try:
        report = Syncer(Transport(), args.evidence_dir).run(load_source(args.root))
    except (SyncError, ValueError, OSError) as exc:
        print(f'SYNC_FAILED: {exc}', file=sys.stderr, flush=True)
        return 1
    summary_path = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary_path:
        with open(summary_path, 'a', encoding='utf-8') as stream:
            stream.write('## Ubuntu digest synchronization verified\n\n')
            stream.write(f"Source dates: {report['sourceDates']}; target dates: {report['archiveDates']}.\n\n")
            for item in report['verified']:
                stream.write(f"- {item['date']}: {item['count']} papers, revision {item['revision']}; complete JSON, IDs, counts, Top3 and repeated-push idempotence verified.\n")
            stream.write(f"\nLatest full readback: {report['latestFullReadback']['date']}. Historical archive preserved.\n")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
