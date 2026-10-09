'use strict';
// Extend the isolated growth stage with the existing Ads UI and a strictly
// read-only bridge. Production HTML and deployment configuration are untouched.
const fs = require('node:fs'), path = require('node:path'), cp = require('node:child_process');
const { builtinModules } = require('node:module');
const root = path.resolve(__dirname, '..');
function readOnlyAdDesignSource(source) {
  function replaceOnce(from, to) {
    const at = source.indexOf(from);
    if (at < 0 || source.indexOf(from, at + from.length) >= 0) throw Error('Saved Ad Design maintenance changed; review its read-only sandbox adaptation.');
    source = source.slice(0, at) + to + source.slice(at + from.length);
  }
  // Keep the saved job/error exactly as it is. Merely opening a sandbox preview
  // must not repair reservations or make a provider request look uncharged.
  const repairStart = source.indexOf('    // This precise legacy error was thrown by buildRequest before responses().');
  const repairEnd = source.indexOf('    }return value; }', repairStart);
  if (repairStart < 0 || repairEnd < repairStart) throw Error('Saved Ad Design recovery changed; review its read-only sandbox adaptation.');
  source = source.slice(0, repairStart) + '    return value; }' + source.slice(repairEnd + '    }return value; }'.length);
  replaceOnce("await ref.collection('imageLibrary').doc(id).set(image);imageLibrary.push", 'imageLibrary.push');
  replaceOnce("    for(const image of imageLibrary.filter(i=>i.kind==='generated'&&i.groupRef===workspace.settings.groupRef&&(productKey(i.ownerProductId)===productKey(workspace.settings.productId)||(i.productIds||[]).some(id=>productKey(id)===productKey(workspace.settings.productId)))))await archiveGenerated(workspace,image);", '    // Read-only preview retains existing images without archiving them.');
  replaceOnce("      await ref.collection('imageLibrary').doc(image.id).set(clean(image));", '      // Read-only preview merges the saved gallery in memory.');
  replaceOnce('await found.ref.update({appearance:item.appearance});', '/* Appearance is computed for this response only. */');
  const start = source.indexOf('  async function status('), end = source.indexOf('  async function workspace(', start);
  if (start < 0 || end < start || /\.(?:set|update|delete|runTransaction)\(/.test(source.slice(start, end))) throw Error('Saved Ad Design status still contains a write; refuse the sandbox stage.');
  return source;
}
function build(out) {
  out = path.resolve(out || path.join(root, 'growth-ads-sandbox-source'));
  if (out === root || !out.startsWith(path.dirname(root) + path.sep)) throw Error('Choose an isolated staging directory.');
  // Refuse to repurpose a stale stage that might contain unrelated entrypoints.
  if (fs.existsSync(path.join(out, 'netlify', 'production-functions'))) fs.rmSync(path.join(out, 'netlify', 'production-functions'), { recursive: true });
  const base = cp.spawnSync(process.execPath, [path.join(__dirname, 'build-growth-sandbox.cjs'), out], { encoding: 'utf8', env: { ...process.env, BRITES_GROWTH_CORE_STAGE: '1' } });
  if (base.status !== 0) throw Error(base.stderr || base.stdout || 'The isolated growth stage could not be built.');
  const publicDir = path.join(out, 'public-site'), modules = new Set(), packages = new Set();
  const builtins = new Set(builtinModules.flatMap(x => [x, 'node:' + x]));
  function copy(relative, destination = relative) {
    const source = path.resolve(root, relative), target = path.resolve(out, destination);
    if (!source.startsWith(root + path.sep) || !target.startsWith(out + path.sep) || !fs.statSync(source).isFile()) throw Error('Unsafe sandbox source path: ' + relative);
    fs.mkdirSync(path.dirname(target), { recursive: true }); fs.copyFileSync(source, target);
  }
  function resolveLocal(source, spec) {
    const file = path.resolve(path.dirname(source), spec);
    for (const candidate of [file, file + '.js', file + '.cjs', file + '.json', path.join(file, 'index.js')]) {
      if (fs.existsSync(candidate) && fs.statSync(candidate).isFile()) return candidate;
    }
    throw Error('A required server module is missing: ' + spec + ' in ' + path.relative(root, source));
  }
  function server(source) {
    const relative = path.relative(root, source);
    if (relative.startsWith('..') || /(?:^|\/)\.env(?:\.|$)/.test(relative)) throw Error('Server dependency escapes source checkout.');
    if (modules.has(relative)) return; modules.add(relative);
    if (relative === 'netlify/functions/firebaseAdmin.js') {
      // No Storage CORS initialization, and every production DB write is denied.
      fs.writeFileSync(path.join(out, relative), "module.exports=require('./_britesGrowthAdsReadOnly.js').adminFromEnvironment(process.env);\n");
      server(path.join(root, 'netlify/functions/_britesGrowthAdsReadOnly.js')); packages.add('firebase-admin'); return;
    }
    copy(relative);
    if (/\.json$/.test(relative)) return;
    const text = fs.readFileSync(source, 'utf8');
    if (relative === 'netlify/functions/googleAdsAdDesign.js') {
      fs.writeFileSync(path.join(out, relative), readOnlyAdDesignSource(text));
    }
    const references = [
      ...text.matchAll(/(?:\brequire\s*\(\s*|\bimport\s*\(\s*)["']([a-zA-Z0-9_./@-]+)["']/g),
      ...text.matchAll(/^\s*(?:import|export)\s+[^\n]*?\bfrom\s+["']([a-zA-Z0-9_./@-]+)["']/gm)
    ].map(m => m[1]);
    for (const spec of references) {
      if (spec.startsWith('.')) server(resolveLocal(source, spec));
      else if (!builtins.has(spec)) packages.add(spec.startsWith('@') ? spec.split('/').slice(0, 2).join('/') : spec.split('/')[0]);
    }
  }
  server(path.join(root, 'netlify/functions/britesGrowthAds.js'));
  // Dynamic imports in the adapter retain their exact literal paths above.
  const wrapper = "import handler from '../functions/britesGrowthAds.js';\nexport default handler;\nexport const config = { path: '/api/growth-ads', method: ['POST'] };\n";
  fs.writeFileSync(path.join(out, 'netlify/production-functions/britesGrowthAds.js'), wrapper);
  const assetSet = new Set();
  function asset(relative) {
    relative = relative.replace(/^\//, '').split(/[?#]/)[0];
    if (!relative || relative.includes('..') || assetSet.has(relative) || !fs.existsSync(path.join(root, relative))) return;
    if (!fs.statSync(path.join(root, relative)).isFile()) return;
    assetSet.add(relative); copy(relative, 'public-site/' + relative);
    if (/\.css$/.test(relative)) {
      const css = fs.readFileSync(path.join(root, relative), 'utf8');
      for (const m of css.matchAll(/url\(\s*["']?([^"')\s]+)|@import\s+["']([^"']+)["']/g)) {
        const url = m[1] || m[2];
        if (!/^(?:https?:|data:|\/\/)/.test(url)) asset(url.startsWith('/') ? url : path.join(path.dirname(relative), url));
      }
    }
  }
  let html = fs.readFileSync(path.join(root, 'brites-adwords.html'), 'utf8');
  for (const m of html.matchAll(/<(?:script|link)\b[^>]*(?:src|href)=["']([^"']+)["'][^>]*>/gi)) {
    if (m[1].startsWith('/')) asset(m[1]);
  }
  for (const filename of ['logo-square.png', 'logo-wide.png', 'icon-blue.png']) {
    asset('assets/brites-brand/' + filename); copy('assets/brites-brand/' + filename);
  }
  const preload = '/brites-growth-ad-sandbox.js';
  fs.writeFileSync(path.join(publicDir, preload.slice(1)), "window.__GADS_ENDPOINT='/api/growth-ads';\nwindow.__BRITES_GROWTH_AD_SANDBOX=true;\n");
  html = html.replace('</head>', '<script src="' + preload + '"></script></head>');
  html = html.replace('This console can change live advertising spend. Authorised operators only.', 'This sandbox reads saved research and Google Ads reports. Changes and paid AI jobs are disabled.');
  const from = html.indexOf('async function livePull(){'), to = html.indexOf('/* Pending budget overrides:', from);
  if (from < 0 || to < from) throw Error('The existing Ads refresh changed; review its sandbox adaptation.');
  html = html.slice(0, from) + 'async function livePull(){\n  if(livePulling)return; livePulling=true; lastLivePull=Date.now();\n  try{DASH=await api("dashboard",{activity:feedBounds()});applyBudgetOverrides();applyEndDateOverrides();renderAll();syncOk();}\n  catch(x){syncFailed(x);}finally{livePulling=false;}\n}\n' + html.slice(to);
  const startup = 'buildNav();bindControlsOnce();renderAll();go("groups");loadDiagnostics();';
  if (!html.includes(startup)) throw Error('The existing Ads startup changed; review its sandbox adaptation.');
  html = html.replace(startup, 'buildNav();bindControlsOnce();renderAll();document.querySelector(\'[data-v="research"]\')?.click();loadDiagnostics();');
  html = html.replace('<title>', '<title>Sandbox · ');
  fs.writeFileSync(path.join(publicDir, 'brites-adwords.html'), html);
  for (const name of ['ads-workspace-browser.html', 'ads-workspace-browser.js']) copy('tests/growth/' + name, 'public-site/sandbox-tests/' + name);
  const rootPackage = JSON.parse(fs.readFileSync(path.join(root, 'package.json'), 'utf8'));
  const manifest = JSON.parse(fs.readFileSync(path.join(out, 'package.json'), 'utf8'));
  for (const name of [...packages].sort()) {
    const version = rootPackage.dependencies?.[name] || rootPackage.devDependencies?.[name];
    if (!version) throw Error('A required server package needs an explicit version: ' + name);
    manifest.dependencies[name] = version;
  }
  manifest.name = 'brites-growth-ads-sandbox';
  fs.writeFileSync(path.join(out, 'package.json'), JSON.stringify(manifest, null, 2) + '\n');
  const native = ['@google-cloud/firestore', 'firebase-admin', 'sharp', '@resvg/resvg-js', '@ffmpeg-installer/ffmpeg'].filter(p => manifest.dependencies[p]);
  let config = fs.readFileSync(path.join(out, 'netlify.toml'), 'utf8');
  config = config.replace(/external_node_modules=\[[^\n]*\]/, 'external_node_modules=' + JSON.stringify(native));
  config = config.replace('[functions]\n', '[build.environment]\n BRITES_GROWTH_SANDBOX="1"\n BRITES_GROWTH_NAMESPACE="Brites_Growth_Sandbox"\n CORS_SET="1"\n[functions]\n');
  config = config.replace(' node_bundler="esbuild"\n', ' node_bundler="esbuild"\n included_files=["assets/brites-brand/**"]\n');
  fs.writeFileSync(path.join(out, 'netlify.toml'), config);
  const entries = fs.readdirSync(path.join(out, 'netlify/production-functions')).sort();
  const expected = ['britesConcierge.js', 'britesConciergeDemoTurn.js', 'britesConciergeMemory.js', 'britesConciergeVoice.js', 'britesConciergeVoiceDeadline-background.js', 'britesConciergeVoiceReaper.js', 'britesGrowthAds.js', 'britesGrowthApi.js', 'britesGrowthCatalogue-background.js', 'britesGrowthCorrections.js', 'britesGrowthTick.js'];
  if (JSON.stringify(entries) !== JSON.stringify(expected)) throw Error('The sandbox contains an unexpected function entrypoint.');
  const summary = { schema: 1, entries, assets: [...assetSet].sort(), privateServerModules: [...modules].sort(), packages: Object.keys(manifest.dependencies).sort(), adsEndpoint: '/api/growth-ads', adsWrites: false, paidAiKeysRequired: false };
  fs.writeFileSync(path.join(out, 'sandbox-ad-manifest.json'), JSON.stringify(summary, null, 2) + '\n');
  return { out, entries, assets: assetSet.size, serverModules: modules.size, packages: summary.packages };
}
if (require.main === module) console.log(JSON.stringify(build(process.argv[2])));
module.exports = { build, readOnlyAdDesignSource };
