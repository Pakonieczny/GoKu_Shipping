'use strict';
const test = require('node:test'), assert = require('node:assert/strict');
const diagnostic = require('../../scripts/inspect-concierge-avatar.cjs');
test('paired digital-eye robot builds real geometry with finite positions, normals and texture coordinates', () => {
  const model = diagnostic.buildModel('high');
  try {
    const result = diagnostic.inspect(model);
    assert.equal(result.finiteBuffers, true); assert.equal(result.validIndices, true);
    assert.ok(result.triangles > 100000); assert.ok(result.vertices > 100000);
    assert.equal(result.rig.eyeAssemblies, 2); assert.equal(result.rig.upperAndLowerLids, 0); assert.equal(result.rig.armAssemblies, 2); assert.equal(result.instancedCopies, 28);
    assert.equal(model.eyes[0].aperture.name, 'expression-eye-left'); assert.equal(model.eyes[1].aperture.name, 'expression-eye-right');
    for (const eye of model.eyes) {assert.equal(eye.digital, true); assert.equal(eye.aperture.geometry.type, 'ExtrudeGeometry');}
    assert.notEqual(model.eyes[0].apertureGeometry, model.eyes[1].apertureGeometry, 'asymmetric expressions use independent geometry');
    assert.equal(model.head.getObjectByName('retired-aperture-ornament').children.length, 0);
    model.avatar.updateMatrixWorld(true); model.avatar.traverse(part => assert.ok(part.matrixWorld.elements.every(Number.isFinite)));
  } finally {diagnostic.dispose(model);}
});
test('adaptive avatar preserves the same digital-eye rig, model parts and chain while reducing tessellation', () => {const high = diagnostic.buildModel('high'), adaptive = diagnostic.buildModel('adaptive'); try {const a = diagnostic.inspect(high), b = diagnostic.inspect(adaptive); assert.deepEqual(a.rig, b.rig); assert.equal(a.meshes, b.meshes); assert.equal(a.instancedCopies, b.instancedCopies); assert.ok(b.triangles < a.triangles); assert.ok(b.triangles > 100000); assert.equal(b.finiteBuffers, true); assert.equal(b.validIndices, true);} finally {diagnostic.dispose(high); diagnostic.dispose(adaptive);}});
