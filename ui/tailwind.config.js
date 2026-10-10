import forms from '@tailwindcss/forms'
import typography from '@tailwindcss/typography'

// Status palettes (emerald = ok, amber = warn, red = danger, sky = info) read
// CSS variables too, so a theme can retune status colors without touching a
// component. Only the shades the UI uses are routed; the defaults in
// index.css are Tailwind's own values.
const STATUS_SHADES = [200, 300, 400, 500, 600, 700, 800]
const statusScale = (name) =>
  Object.fromEntries(STATUS_SHADES.map((shade) => [shade, `rgb(var(--c-${name}-${shade}) / <alpha-value>)`]))

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
        emerald: statusScale('ok'),
        amber: statusScale('warn'),
        red: statusScale('danger'),
        sky: statusScale('info'),
      },
      // Families are CSS variables (set in index.css) so a theme swaps type
      // without a rebuild.
      fontFamily: {
        sans: ['var(--font-sans)'],
        display: ['var(--font-display)'],
        // Fira Code ships programming ligatures on by default; IDs, hashes and
        // log lines read better without them, so `font-mono` turns off both the
        // standard (liga) and contextual (calt) ligature features.
        mono: ['var(--font-mono)', { fontFeatureSettings: '"liga" 0, "calt" 0' }],
      },
      fontSize: {
        '2xs': ['0.6875rem', { lineHeight: '1rem' }],
      },
      borderRadius: {
        sm: 'var(--r-sm)',
        DEFAULT: 'var(--r)',
        md: 'var(--r-md)',
        lg: 'var(--r-lg)',
        xl: 'var(--r-xl)',
        '2xl': 'var(--r-2xl)',
        full: 'var(--r-full)',
      },
      boxShadow: {
        card: 'var(--shadow-card)',
        pop: 'var(--shadow-pop)',
      },
    },
  },
  plugins: [forms({ strategy: 'class' }), typography],
}
