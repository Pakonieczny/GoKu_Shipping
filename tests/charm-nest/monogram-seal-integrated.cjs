// MONOSEAL2 on MONOSEAL's REAL readers: the same surfaces test (monogram-seal-surfaces.cjs) with CharmNestMonogramUI.sealsOf / sealHtml and the real Seal markup instead of stand-ins (the order's timeline events are fed to the real service).
//   node tests/charm-nest/monogram-seal-integrated.cjs [playwright-core dir]
process.env.MONO_REAL = '1'; require('./monogram-seal-surfaces.cjs');
