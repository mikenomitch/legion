import { createSignal } from 'solid-js'

export type CommandPaletteIndexItem = {
  id: string
  label: string
  subtitle?: string
  href: string
  source: string
}

const [commandPaletteIndexItems, _setCommandPaletteIndexItems] = createSignal<CommandPaletteIndexItem[]>([])

export const setCommandPaletteIndexItems = (items: readonly CommandPaletteIndexItem[]) => {
  _setCommandPaletteIndexItems(Array.from(items))
}

export const clearCommandPaletteIndexItems = () => {
  _setCommandPaletteIndexItems([])
}

export { commandPaletteIndexItems }
