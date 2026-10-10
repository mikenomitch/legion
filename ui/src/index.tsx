/* @refresh reload */
import { render } from 'solid-js/web'

import './index.css'
import App from './App'

const root = document.getElementById('root')

const mount = () => render(() => <App />, root!)

// The design lab (src/themes) previews candidate themes on the dev server:
// in `npm run lab`, with `?design=` in the URL, or once the studio has been
// used in this browser. `import.meta.env.DEV` is statically false in
// production builds, so the import — themes, fonts and all — is dropped.
const wantsDesignLab = () => {
  if (import.meta.env.MODE === 'lab' || import.meta.env.MODE === 'record') return true
  if (new URLSearchParams(window.location.search).has('design')) return true
  try {
    return localStorage.getItem('ui-design-studio') !== null
  } catch {
    return false
  }
}

if (import.meta.env.DEV && wantsDesignLab()) {
  import('./themes/lab')
    .then(({ initDesignLab }) => initDesignLab())
    .catch((error) => console.warn('design lab failed to load', error))
    .finally(mount)
} else {
  mount()
}
