// Font-stack builders shared by the theme catalogs. The first family is what
// fontOptions.ts matches on, so keep these in step with its helpers.
export const stack = (primary: string, fallback = 'system-ui, -apple-system, sans-serif') => `'${primary}', ${fallback}`
export const monoStack = (primary: string) => `'${primary}', ui-monospace, SFMono-Regular, Menlo, monospace`
