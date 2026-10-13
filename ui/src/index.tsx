/* @refresh reload */
import { render } from 'solid-js/web'

import './index.css'
import './appearance/themes.css'
import App from './App'

const root = document.getElementById('root')

render(() => <App />, root!)
