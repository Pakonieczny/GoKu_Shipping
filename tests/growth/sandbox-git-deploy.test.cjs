'use strict';
const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { assertSandboxBuild } = require('../../growth/build-sandbox.cjs');
const root = path.resolve(__dirname, '../..');
const out = path.join(root, 'growth-sandbox-source');
const env = { ...process.env, NETLIFY: 'true', BRANCH: 'codex/brites-growth-2026-10',
  BRITES_GROWTH_SANDBOX: '1', BRITES_GROWTH_NAMESPACE: 'Brites_Growth_Sandbox' };

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
  const result = spawnSync(process.execPath, ['growth/build-sandbox.cjs'], {
    cwd: root, env, encoding: 'utf8'
  });
  assert.equal(result.status, 0, result.stderr + result.stdout);
  assert.equal(fs.existsSync(staleAsset), false);
  assert.equal(fs.existsSync(staleFunction), false);
  const entries = fs.readdirSync(path.join(out, 'netlify/production-functions')).sort();
  assert.deepEqual(entries, ['britesConcierge.js', 'britesConciergeDemoTurn.js', 'britesConciergeVoice.js',
    'britesConciergeVoiceDeadline-background.js', 'britesConciergeVoiceReaper.js', 'britesGrowthAds.js',
    'britesGrowthApi.js', 'britesGrowthCatalogue-background.js', 'britesGrowthCorrections.js', 'britesGrowthTick.js']);
  for (const file of ['concierge-sandbox.html', 'brites-concierge-avatar.js',
    'brites-concierge-voice.js', 'brites-concierge-guide.js', 'assets/brites-concierge-avatar-scene.mjs']) {
    assert.deepEqual(fs.readFileSync(path.join(out, 'public-site', file)), fs.readFileSync(path.join(root, file)));
  }
  for (const [file, schedule] of [['britesConciergeVoiceReaper.js', "'* * * * *'"], ['britesGrowthTick.js', "'@hourly'"]]) {
    assert.ok(fs.readFileSync(path.join(out, 'netlify/production-functions', file), 'utf8').includes('schedule:' + schedule)
      || fs.readFileSync(path.join(out, 'netlify/production-functions', file), 'utf8').includes('schedule: ' + schedule));
  }
  assert.equal(fs.existsSync(path.join(out, 'public-site/netlify')), false);
  assert.equal(fs.existsSync(path.join(out, 'public-site/growth/README.md')), false);
  assert.deepEqual(fs.readFileSync(path.join(root, 'netlify.toml')), rootConfig);
});
