import forms from '@tailwindcss/forms'
import typography from '@tailwindcss/typography'

/** @type {import('tailwindcss').Config} */
export default {
  darkMode: 'class',
  content: [
    './index.html',
    './src/**/*.{js,ts,jsx,tsx}',
  ],
  theme: {
    extend: {
      // Theme-aware colors are driven by CSS variables defined in index.css so
      // light/dark switch consistently without per-class dark: overrides.
      colors: {
        canvas: 'rgb(var(--c-canvas) / <alpha-value>)',
        surface: 'rgb(var(--c-surface) / <alpha-value>)',
        surface2: 'rgb(var(--c-surface2) / <alpha-value>)',
        line: 'rgb(var(--c-line) / <alpha-value>)',
        'line-strong': 'rgb(var(--c-line-strong) / <alpha-value>)',
        ink: 'rgb(var(--c-ink) / <alpha-value>)',
        muted: 'rgb(var(--c-muted) / <alpha-value>)',
        faint: 'rgb(var(--c-faint) / <alpha-value>)',
        brass: {
          DEFAULT: 'rgb(var(--c-brass) / <alpha-value>)',
          strong: 'rgb(var(--c-brass-strong) / <alpha-value>)',
        },
      },
      fontFamily: {
        sans: ['"Inter Variable"', 'Inter', 'system-ui', '-apple-system', 'sans-serif'],
        display: ['"Space Grotesk Variable"', '"Space Grotesk"', '"Inter Variable"', 'system-ui', 'sans-serif'],
        // Fira Code ships programming ligatures on by default; IDs, hashes and
        // log lines read better without them, so `font-mono` turns off both the
        // standard (liga) and contextual (calt) ligature features.
        mono: [
          ['"Fira Code Variable"', '"Fira Code"', 'ui-monospace', 'SFMono-Regular', 'Menlo', 'monospace'],
          { fontFeatureSettings: '"liga" 0, "calt" 0' },
        ],
      },
      fontSize: {
        '2xs': ['0.6875rem', { lineHeight: '1rem' }],
      },
      boxShadow: {
        card: '0 1px 2px 0 rgb(0 0 0 / 0.04)',
        pop: '0 12px 32px -12px rgb(0 0 0 / 0.35)',
      },
    },
  },
  plugins: [forms({ strategy: 'class' }), typography],
}
