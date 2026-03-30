import { createViewer3DInstance as cv3, CherryGLVersion } from '@metavrse-inc/metavrse-lib';

// ─── Viewer init ─────────────────────────────────────────────────────────────

const initViewer = async (canvas) => {
  const viewer = await cv3(canvas);
  window.Module = viewer;
  viewer.ProjectManager.path = '/project/files/';
  return viewer;
};

const createWorld = (viewer) => {
  viewer.FS.createPath('/', '/project/files/');
  const projectData = {
    data: {
      version: CherryGLVersion,
      title: 'New Project',
      scene: {
        scene1: {
          tree: [{ key: 'world', type: 'world', title: 'World' }],
          data: {
            world: {
              skybox: { key: '', show: true },
              color: [0, 0, 0],
              transparent: false,
              skyboxRotation: [0, 0, 0],
              shadow: { level: 0, enabled: false, position: [1, 1, 2], fov: false, texture: [1024, 1024] },
              controller: '', fps: 30, dpr: 0.25, fxaa: 1,
              orientation: 1, hudscale: 1, css: '',
              physics_debug_level: 0,
              fov_size: [500, 500, 500], render_method: 0,
              fov_enabled: false, lod_enabled: false,
              zip_enabled: false, zip_size: [1000, 1000, 1000],
            },
          },
        },
      },
      starting_scene: 'scene1',
      assets: { tree: [], data: {} },
      selected_scene: 'scene1',
    },
  };
  viewer.ProjectManager.loadScene(projectData, false);
};

// ─── File handler ─────────────────────────────────────────────────────────────

let viewer = null;

const handleFile = async (file) => {
  if (!file) return;

  const overlay = document.getElementById('drop-overlay');
  overlay.classList.add('hidden');

  if (!viewer) {
    const canvas = document.getElementById('viewer');
    viewer = await initViewer(canvas);
    viewer.getSurface().getScene().showRulerGrid(true);
    createWorld(viewer);
  }

  // Read raw buffer and log it
  const arrayBuffer = await file.arrayBuffer();
  const uint8 = new Uint8Array(arrayBuffer);

  Module.FS.writeFile(
    Module.ProjectManager.path + "project.zip",
    uint8
  );

  // ── Load project from zip ──────────────────────────────────────────────────

  const mergeConfigurationsIntoTree = (tree, configurations) => {
    const flattenTree = (tree) => {
      const flatten = [];
      const deepSearch = (nodes, parentKey) => {
        nodes.forEach((node, index) => {
          let newNode = null;
          let buildNode = null;
          if (node) {
            newNode = { ...node, children: [] };
            buildNode = { ...newNode, index, parent: parentKey };
          }
          flatten.push(buildNode);
          if (node.children.length) deepSearch(node.children, node.key);
        });
      };
      deepSearch(tree, '');
      return flatten;
    };

    const createTree = (flattenArray, parent = '') => {
      const newArr = [];
      flattenArray.forEach((c) => {
        if (parent === c.parent) {
          const { key, ikey, skey, title, type, id, visible } = c;
          const newNode = { key, ikey, skey, title, type, visible };
          if (id) newNode.id = id;
          newArr.push({ ...newNode, children: [...createTree(flattenArray, c.key)] });
        }
      });
      return newArr;
    };

    const merge = (tree, configurations) => {
      let newFlattenTree = [];
      const flatTree = flattenTree(tree);
      const flatConfs = flattenTree(configurations);
      if (configurations.length) {
        newFlattenTree = flatTree;
        const treeMap = new Map(flatTree.map((c) => [c.key, c]));
        flatConfs.forEach((t) => { if (!treeMap.has(t.key)) newFlattenTree.push(t); });
      } else {
        newFlattenTree = flatTree;
      }
      return createTree(newFlattenTree);
    };

    return merge(tree, configurations);
  };

  const scene = viewer.getSurface().getScene();
  const archive = Module.ProjectManager.archive;
  archive.close();
  archive.open(Module.ProjectManager.path + 'project.zip');
  scene.setFSZip(archive);

  const readJsonFile = (filename) => JSON.parse(archive.fopens(filename));

  const project      = readJsonFile('project.json');
  const assets       = readJsonFile('assets.json');
  const { startingScene } = project;
  const tree         = readJsonFile(`scenes/${startingScene}/tree.json`);
  const entities     = readJsonFile(`scenes/${startingScene}/entities.json`);
  const world        = readJsonFile(`scenes/${startingScene}/world.json`);
  const configurations = readJsonFile(`scenes/${startingScene}/configurations.json`);
  const hudTree      = readJsonFile(`scenes/${startingScene}/hud-tree.json`);

  const projectData = {
    data: {
      version: project.version,
      title: project.title,
      scene: {
        [project.startingScene]: {
          tree: [
            ...mergeConfigurationsIntoTree(tree, configurations),
            ...hudTree,
          ],
          data: {
            world,
            ...entities,
          },
        },
      },
      starting_scene: project.startingScene,
      assets: {
        tree: [...assets],
        data: {},
      },
      selected_scene: project.selectedScene,
    },
  };

  Module.ProjectManager.loadScene(projectData, true);

  console.log('=== Dropped file ===');
  console.log('Name:', file.name);
  console.log('Size:', file.size, 'bytes');
  console.log('Buffer (Uint8Array):', uint8);
  console.log('ArrayBuffer:', arrayBuffer);
};

// ─── Drag & Drop ─────────────────────────────────────────────────────────────

window.addEventListener('DOMContentLoaded', () => {
  const overlay   = document.getElementById('drop-overlay');
  const dropZone  = document.getElementById('drop-zone');
  const fileInput = document.getElementById('file-input');

  document.addEventListener('dragover', (e) => {
    e.preventDefault();
    overlay.classList.remove('hidden');
    overlay.classList.add('drag-over');
  });

  document.addEventListener('dragleave', (e) => {
    if (!e.relatedTarget) overlay.classList.remove('drag-over');
  });

  document.addEventListener('drop', (e) => {
    e.preventDefault();
    overlay.classList.remove('drag-over');
    handleFile(e.dataTransfer.files[0]);
  });

  dropZone.addEventListener('click', () => fileInput.click());
  fileInput.addEventListener('change', (e) => handleFile(e.target.files[0]));
});
