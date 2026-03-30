import { createViewer3DInstance as cv3, CherryGLVersion } from '@metavrse-inc/metavrse-lib';

const PROJECT_MANAGER_PATH = '/project/files/';

const initViewer = async (canvas) => {
  const viewer = await cv3(canvas);
  window.Module = viewer;
  viewer.ProjectManager.path = PROJECT_MANAGER_PATH;
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
              shadow: {
                level: 0,
                enabled: false,
                position: [1, 1, 2],
                fov: false,
                texture: [1024, 1024],
              },
              controller: '',
              fps: 30,
              dpr: 0.25,
              fxaa: 1,
              orientation: 1,
              hudscale: 1,
              css: '',
              physics_debug_level: 0,
              fov_size: [500, 500, 500],
              render_method: 0,
              fov_enabled: false,
              lod_enabled: false,
              zip_enabled: false,
              zip_size: [1000, 1000, 1000],
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

// ── Boot ──────────────────────────────────────────────────────────────────────

const canvas = document.getElementById('viewer');

initViewer(canvas)
  .then((viewer) => {
    viewer.getSurface().getScene().showRulerGrid(true);
    createWorld(viewer);
    console.log('Cherry3D viewer ready');
  })
  .catch((err) => {
    console.error('Cherry3D init failed:', err);
  });
