#!/usr/bin/env python3
"""Run the private native compatibility checks in an explicit candidate environment.

This is focused evidence, not a full-suite or production activation receipt.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def capture(argv, env):
    return subprocess.check_output(argv, cwd=ROOT, env=env, text=True, stderr=subprocess.STDOUT, timeout=30).strip()


def source_digest(env):
    tracked = capture(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], env)
    digest = hashlib.sha256()
    for name in sorted(set(tracked.split('\0'))):
        path = ROOT / name
        if name and path.is_file():
            digest.update(name.encode() + b'\0' + hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kernel-python', required=True, type=Path)
    parser.add_argument('--ts-reference', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--timeout-seconds', type=int, default=900)
    parser.add_argument('--preflight-only', action='store_true')
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error('--timeout-seconds must be positive')
    out = args.output.resolve()
    if out.is_relative_to(ROOT):
        parser.error('--output must be outside the source checkout')
    if out.exists():
        parser.error('--output must be a fresh directory; previous evidence is never overwritten')
    out.mkdir(parents=True)
    env = dict(os.environ, TZ='UTC', DO_NOT_TRACK='1', CARGO_BUILD_JOBS='4',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0')
    # Different source roots must never share workspace-crate fingerprints.
    # Retain the literal target component used by native fixture discovery.
    target = Path(env.get('CARGO_TARGET_DIR', str(ROOT.parent / '.private-build' / ROOT.name / 'target'))).resolve()
    target.mkdir(parents=True, exist_ok=True)
    stamp = target / '.private-source-root'
    owner = str(ROOT.resolve()) + '\n'
    if stamp.exists() and stamp.read_text() != owner:
        raise RuntimeError(f'target belongs to a different checkout: {target}')
    if not stamp.exists() and any(p.name != '.private-source-root' for p in target.iterdir()):
        raise RuntimeError('use a fresh target directory for the first private check run')
    stamp.write_text(owner)
    kernel = args.kernel_python.absolute()
    reference = args.ts_reference.absolute()
    env.update(CARGO_TARGET_DIR=str(target), PA_E2E_KERNEL_PYTHON=str(kernel),
               PA_TS_REFERENCE=str(reference), FACTORY_FIXTURE_ROOT=str(out / 'factory'))
    probe = json.loads(capture([str(kernel), '-c',
        'import json,rlm,dill; print(json.dumps({"runtime":rlm.__file__}))'], env))
    runtime = Path(probe['runtime']).resolve()
    if not runtime.is_relative_to(ROOT / 'prime-agent-runtime'):
        raise RuntimeError(f'kernel imports another checkout: {runtime}')
    ts_version = capture([str(reference), '--version'], env)
    if '0.9.8' not in ts_version:
        raise RuntimeError(f'expected TS 0.9.8 reference, got {ts_version!r}')
    receipt = {'scope': 'focused-private-native-compatibility', 'productionQualified': False,
               'sourceRoot': str(ROOT), 'head': capture(['git', 'rev-parse', 'HEAD'], env),
               'sourceDigest': source_digest(env), 'kernel': str(kernel), 'runtime': str(runtime),
               'tsVersion': ts_version, 'target': str(target), 'checks': [], 'passed': False}
    report = out / 'result.json'
    def save():
        report.write_text(json.dumps(receipt, indent=2) + '\n')
    save()
    if args.preflight_only:
        receipt['preflightPassed'] = True
        save()
        return 0
    checks = [('fmt', ['cargo', 'fmt', '--all', '--check']),
              ('clippy', ['cargo', 'clippy', '--workspace', '--all-targets', '--', '-D', 'warnings'])]
    for selector in ['child_work::', 'request_progress_', 'shutdown_', 'update_snapshot', 'drone_control::']:
        checks.append((selector.replace(':', ''), ['cargo', 'test', '-p', 'pa-daemon', '--lib', selector, '--', '--test-threads=2']))
    for suite in ['rlm_children_replacement_e2e', 'rlm_roster_walk_e2e',
                  'factory_mvp_e2e', 'orchestration_gate_e2e']:
        checks.append((suite, ['cargo', 'test', '-p', 'pa-daemon', '--test', suite, '--', '--test-threads=1']))
    for name, argv in checks:
        started = time.monotonic()
        with (out / (name + '.log')).open('w') as log:
            proc = subprocess.Popen(argv, cwd=ROOT, env=env, stdout=log,
                                    stderr=subprocess.STDOUT, start_new_session=True)
            timed_out = False
            try:
                code = proc.wait(timeout=args.timeout_seconds)
            except subprocess.TimeoutExpired:
                timed_out = True
                os.killpg(proc.pid, signal.SIGKILL)
                code = proc.wait()
        content = (out / (name + '.log')).read_text()
        observed_tests = 'cargo' == argv[0] and 'test' in argv
        no_tests = observed_tests and ('test result: ok. 0 passed' in content or 'test result: ok.' not in content)
        receipt['checks'].append({'name': name, 'command': argv, 'exitCode': code,
                                  'timedOut': timed_out, 'missingTests': no_tests,
                                  'seconds': round(time.monotonic() - started, 2)})
        save()
        if code or timed_out or no_tests:
            return 1
    receipt['sourceUnchanged'] = source_digest(env) == receipt['sourceDigest']
    receipt['passed'] = receipt['sourceUnchanged']
    save()
    return 0 if receipt['passed'] else 1


if __name__ == '__main__':
    sys.exit(main())
