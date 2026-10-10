'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');
const { spawnSync } = require('node:child_process');
const { assertSandboxBuild } = require('../../growth/build-sandbox.cjs');
const root = path.resolve(__dirname, '../..');
const out = path.join(root, 'growth-sandbox-source');
const env = { ...process.env, NETLIFY: 'true', BRANCH: 'codex/brites-growth-2026-10',
  BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Sandbox' };

test('an avatar-only parent change rebuilds the sandbox while unchanged sources skip it', () => {
  const config = fs.readFileSync(path.join(root, 'growth/netlify.toml'), 'utf8');
  const ignore = config.match(/^\s*ignore = '([^']+)'/m)?.[1];
  assert.ok(ignore, 'the sandbox needs an explicit parent-source change check');
  const repo = fs.mkdtempSync(path.join(os.tmpdir(), 'brites-deploy-filter-'));
  function git(...args) {
    const result = spawnSync('git', args, { cwd: repo, encoding: 'utf8' });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  }
  try {
    fs.mkdirSync(path.join(repo, 'growth'));
    fs.writeFileSync(path.join(repo, 'growth/fixture'), 'build source');
    fs.writeFileSync(path.join(repo, 'brites-concierge-avatar.js'), 'first avatar');
    git('init', '-q');
    git('config', 'user.name', 'Sandbox Fixture');
    git('config', 'user.email', 'sandbox@example.invalid');
    git('add', '.');
    git('commit', '-qm', 'initial source');
    const previous = git('rev-parse', 'HEAD');
    fs.writeFileSync(path.join(repo, 'brites-concierge-avatar.js'), 'updated avatar');
    git('commit', '-qam', 'avatar-only change');
    const current = git('rev-parse', 'HEAD');
    const run = (cached, commit) => spawnSync('bash', ['-c', ignore], {
      cwd: path.join(repo, 'growth'), encoding: 'utf8',
      env: { ...process.env, CACHED_COMMIT_REF: cached, COMMIT_REF: commit }
    });
    assert.equal(run(previous, current).status, 1, 'parent avatar changes must build');
    assert.equal(run(current, current).status, 0, 'unchanged sources can skip');
    assert.equal(run('', current).status, 1, 'missing cache metadata must build');
  } finally { fs.rmSync(repo, { recursive: true, force: true }); }
});

test('Netlify rejects a different branch or namespace before touching the stage', () => {
  const marker = path.join(out, 'guard-test-marker');
  fs.mkdirSync(out, { recursive: true });
  fs.writeFileSync(marker, 'preserve until a valid build');
  try {
    for (const override of [{ BRANCH: 'main' }, { BRANCH: '' },
      { BRITES_GROWTH_NAMESPACE: 'Production' }, { BRITES_GROWTH_SANDBOX: '0' }]) {
      const result = spawnSync(process.execPath, ['growth/build-sandbox.cjs'], {
        cwd: root, env: { ...env, ...override }, encoding: 'utf8'
      });
      assert.notEqual(result.status, 0);
      assert.equal(fs.readFileSync(marker, 'utf8'), 'preserve until a valid build');
    }
    assert.doesNotThrow(() => assertSandboxBuild(env));
    assert.doesNotThrow(() => assertSandboxBuild({}));
  } finally { fs.rmSync(marker, { force: true }); }
});

test('the actual Git build removes stale output and retains only the complete isolated sandbox', () => {
  const staleAsset = path.join(out, 'public-site', 'unrelated-old-app.html');
  const staleFunction = path.join(out, 'netlify/production-functions', 'unrelated-schedule.js');
  for (const file of [staleAsset, staleFunction]) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    fs.writeFileSync(file, 'must not be deployed');
  }
  const rootConfig = fs.readFileSync(path.join(root, 'netlify.toml'));
  const result = spawnSync(process.execPath, ['build-sandbox.cjs'], {
    cwd: path.join(root, 'growth'), env, encoding: 'utf8'
  });
  assert.equal(result.status, 0, result.stderr + result.stdout);
  assert.equal(fs.existsSync(staleAsset), false);
  assert.equal(fs.existsSync(staleFunction), false);
  const entries = fs.readdirSync(path.join(out, 'netlify/production-functions')).sort();
  assert.deepEqual(entries, ['britesConcierge.js', 'britesConciergeDemoTurn.js', 'britesConciergeMemory.js', 'britesConciergeVoice.js',
    'britesConciergeVoiceDeadline-background.js', 'britesConciergeVoiceReaper.js', 'britesGrowthAds.js',
    'britesGrowthApi.js', 'britesGrowthCatalogue-background.js', 'britesGrowthCorrections.js', 'britesGrowthTick.js', 'deploy-succeeded.js']);
  for (const file of ['concierge-sandbox.html', 'brites-concierge-avatar.js',
    'brites-concierge-voice.js', 'brites-concierge-memory-config.js', 'brites-concierge-memory.js', 'brites-concierge-guide.js', 'assets/brites-concierge-avatar-scene.mjs']) {
    assert.deepEqual(fs.readFileSync(path.join(out, 'public-site', file)), fs.readFileSync(path.join(root, file)));
  }
  for (const [file, schedule] of [['britesConciergeVoiceReaper.js', "'* * * * *'"], ['britesGrowthTick.js', "'@hourly'"]]) {
    assert.ok(fs.readFileSync(path.join(out, 'netlify/production-functions', file), 'utf8').includes('schedule:' + schedule)
      || fs.readFileSync(path.join(out, 'netlify/production-functions', file), 'utf8').includes('schedule: ' + schedule));
  }
  assert.equal(fs.existsSync(path.join(out, 'public-site/netlify')), false);
  assert.equal(fs.existsSync(path.join(out,'public-site/_britesCharmStoryBootstrap.json')),false);
  assert.equal(fs.existsSync(path.join(out,'public-site/_britesConciergeLibraryDeploy.js')),false);
  assert.deepEqual(fs.readFileSync(path.join(out,'netlify/functions/_britesConciergeLibraryDeploy.js')),fs.readFileSync(path.join(root,'netlify/functions/_britesConciergeLibraryDeploy.js')));
  const deployEvent=fs.readFileSync(path.join(out,'netlify/production-functions/deploy-succeeded.js'),'utf8');
  assert.match(deployEvent,/export default handler/);assert.doesNotMatch(deployEvent,/config\s*=|schedule\s*:/);
  const memoryRoute = fs.readFileSync(path.join(out, 'netlify/production-functions/britesConciergeMemory.js'), 'utf8');
  assert.match(memoryRoute, /path:\s*['"]\/api\/concierge-memory['"]/);
  assert.deepEqual(fs.readFileSync(path.join(out, 'netlify/functions/_britesConciergeMemory.js')),
    fs.readFileSync(path.join(root, 'netlify/functions/_britesConciergeMemory.js')));
  assert.equal(fs.existsSync(path.join(out, 'public-site/_britesConciergeMemory.js')), false);
  const revisionModule = path.join(out, 'netlify/functions/_britesGrowthKeywordRevision.js');
  assert.equal(typeof require(revisionModule).createKeywordRevision, 'function',
    'the protected revision route needs its runtime dependency in the deployed stage');
  assert.equal(fs.existsSync(path.join(out, 'public-site/_britesGrowthKeywordRevision.js')), false);
  assert.equal(fs.existsSync(path.join(out, 'public-site/growth/README.md')), false);
  assert.deepEqual(fs.readFileSync(path.join(root, 'netlify.toml')), rootConfig);
});
