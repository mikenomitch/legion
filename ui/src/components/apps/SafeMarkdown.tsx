import { A } from '@solidjs/router'
import { For, Match, Switch, type JSX } from 'solid-js'
import { isHref, isRoute } from '../../utils/appViews'
import { linkClass } from '../ui'

/*
 * The `markdown` block of an app view: a CommonMark subset drawn as DOM
 * nodes, never as HTML. Headings, paragraphs, lists, block quotes, code
 * fences, inline code, emphasis, and links. Links go to in-app routes or open
 * http(s) URLs in a new tab; anything else renders as its text. The manager
 * has already refused raw HTML and images; this renderer could not draw them
 * anyway.
 */

type MdBlock =
  | { kind: 'heading'; level: number; text: string }
  | { kind: 'paragraph'; text: string }
  | { kind: 'list'; ordered: boolean; items: string[] }
  | { kind: 'quote'; text: string }
  | { kind: 'code'; text: string }

export function parseMarkdown(source: string): MdBlock[] {
  const lines = source.replace(/\r\n?/g, '\n').split('\n')
  const blocks: MdBlock[] = []
  let paragraph: string[] = []
  const flush = () => {
    if (paragraph.length > 0) {
      blocks.push({ kind: 'paragraph', text: paragraph.join(' ') })
      paragraph = []
    }
  }
  for (let index = 0; index < lines.length; index++) {
    const line = lines[index]
    const trimmed = line.trim()
    if (trimmed.startsWith('```')) {
      flush()
      const code: string[] = []
      index++
      while (index < lines.length && !lines[index].trim().startsWith('```')) {
        code.push(lines[index])
        index++
      }
      blocks.push({ kind: 'code', text: code.join('\n') })
      continue
    }
    if (trimmed === '') {
      flush()
      continue
    }
    const heading = /^(#{1,6})\s+(.*)$/.exec(trimmed)
    if (heading) {
      flush()
      blocks.push({ kind: 'heading', level: heading[1].length, text: heading[2] })
      continue
    }
    const bullet = /^[-*+]\s+(.*)$/.exec(trimmed)
    const numbered = /^\d+[.)]\s+(.*)$/.exec(trimmed)
    if (bullet || numbered) {
      flush()
      const ordered = Boolean(numbered)
      const items = [(bullet ?? numbered)![1]]
      while (index + 1 < lines.length) {
        const next = lines[index + 1].trim()
        const more = ordered ? /^\d+[.)]\s+(.*)$/.exec(next) : /^[-*+]\s+(.*)$/.exec(next)
        if (!more) break
        items.push(more[1])
        index++
      }
      blocks.push({ kind: 'list', ordered, items })
      continue
    }
    if (trimmed.startsWith('>')) {
      flush()
      const quoted = [trimmed.replace(/^>\s?/, '')]
      while (index + 1 < lines.length && lines[index + 1].trim().startsWith('>')) {
        quoted.push(lines[index + 1].trim().replace(/^>\s?/, ''))
        index++
      }
      blocks.push({ kind: 'quote', text: quoted.join(' ') })
      continue
    }
    paragraph.push(trimmed)
  }
  flush()
  return blocks
}

type Inline =
  | { kind: 'text'; text: string }
  | { kind: 'code'; text: string }
  | { kind: 'strong'; text: string }
  | { kind: 'em'; text: string }
  | { kind: 'link'; text: string; target: string }

export function parseInline(text: string): Inline[] {
  const pattern = /`([^`]+)`|\*\*([^*]+)\*\*|__([^_]+)__|\*([^*]+)\*|_([^_]+)_|\[([^\]]+)\]\(\s*<?([^)\s>]*)>?\s*\)/g
  const out: Inline[] = []
  let last = 0
  for (const match of text.matchAll(pattern)) {
    const at = match.index ?? 0
    if (at > last) out.push({ kind: 'text', text: text.slice(last, at) })
    if (match[1] !== undefined) out.push({ kind: 'code', text: match[1] })
    else if (match[2] !== undefined || match[3] !== undefined) out.push({ kind: 'strong', text: match[2] ?? match[3] })
    else if (match[4] !== undefined || match[5] !== undefined) out.push({ kind: 'em', text: match[4] ?? match[5] })
    else out.push({ kind: 'link', text: match[6], target: match[7] })
    last = at + match[0].length
  }
  if (last < text.length) out.push({ kind: 'text', text: text.slice(last) })
  return out
}

function InlineText(props: { text: string }): JSX.Element {
  return (
    <For each={parseInline(props.text)}>
      {(part) => (
        <Switch fallback={<>{part.text}</>}>
          <Match when={part.kind === 'code'}>
            <code class="rounded bg-surface2 px-1 font-mono text-[0.85em]">{part.text}</code>
          </Match>
          <Match when={part.kind === 'strong'}>
            <strong class="font-semibold">{part.text}</strong>
          </Match>
          <Match when={part.kind === 'em'}>
            <em>{part.text}</em>
          </Match>
          <Match when={part.kind === 'link' && isRoute((part as { target: string }).target)}>
            <A href={(part as { target: string }).target} class={linkClass}>
              {part.text}
            </A>
          </Match>
          <Match when={part.kind === 'link' && isHref((part as { target: string }).target)}>
            <a href={(part as { target: string }).target} target="_blank" rel="noopener noreferrer" class={linkClass}>
              {part.text}
            </a>
          </Match>
        </Switch>
      )}
    </For>
  )
}

export function SafeMarkdown(props: { text: string }) {
  return (
    <div class="space-y-2 text-sm leading-relaxed text-ink">
      <For each={parseMarkdown(props.text)}>
        {(block) => (
          <Switch>
            <Match when={block.kind === 'heading' ? block : undefined}>
              {(heading) => (
                <p class={heading().level <= 2 ? 'font-display text-base font-semibold' : 'text-sm font-semibold'}>
                  <InlineText text={heading().text} />
                </p>
              )}
            </Match>
            <Match when={block.kind === 'paragraph' ? block : undefined}>
              {(paragraph) => (
                <p>
                  <InlineText text={paragraph().text} />
                </p>
              )}
            </Match>
            <Match when={block.kind === 'list' ? block : undefined}>
              {(list) => {
                const items = (
                  <For each={list().items}>
                    {(item) => (
                      <li>
                        <InlineText text={item} />
                      </li>
                    )}
                  </For>
                )
                return list().ordered ? <ol class="ml-5 list-decimal space-y-0.5">{items}</ol> : <ul class="ml-5 list-disc space-y-0.5">{items}</ul>
              }}
            </Match>
            <Match when={block.kind === 'quote' ? block : undefined}>
              {(quote) => (
                <blockquote class="border-l-2 border-line-strong pl-3 text-muted">
                  <InlineText text={quote().text} />
                </blockquote>
              )}
            </Match>
            <Match when={block.kind === 'code' ? block : undefined}>
              {(code) => <pre class="overflow-x-auto rounded-md bg-surface2/70 p-2 font-mono text-xs">{code().text}</pre>}
            </Match>
          </Switch>
        )}
      </For>
    </div>
  )
}
