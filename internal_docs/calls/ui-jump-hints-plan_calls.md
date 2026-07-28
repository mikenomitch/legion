# Judgement Calls — UI Jump Hints

Plan: [`../plans/ui-jump-hints-plan.md`](../plans/ui-jump-hints-plan.md).

Ordered by project impact, highest first. The entry number is its rank.

## 1. Sidebar letters take `⌘⇧`, index digits take `⌘` alone

The request was "hold command, see numbers on rows and letters in the sidebar,
press command+key to jump", with explicit permission to add a modifier if the
plain one clashed. Rows kept the plain modifier. Sidebar letters gained Shift,
and the badge spells the whole chord:

```
Jobs                                  ⇧B      ← press ⌘⇧B
1  svc-api          DONE                      ← press ⌘1
```

**The alternatives.**

- *Plain `⌘`+letter for the sidebar*, as literally asked. `⌘S`, `⌘W`, `⌘R`,
  `⌘O`, `⌘P`, `⌘A`, `⌘F` are the browser's and the OS's, several of them
  non-negotiably (`⌘W` closes the tab). There is no set of mnemonic letters
  left, and a shortcut that sometimes closes your tab is worse than no shortcut.
- *`⌘⇧` for everything, digits included.* Uniform and easy to explain — one
  chord, two alphabets. Rejected because `⌘⇧3`, `⌘⇧4` and `⌘⇧5` are macOS
  screenshot shortcuts handled above the browser: three of the ten row hints
  would silently take a screenshot instead.
- *`Ctrl⇧` for everything, on macOS too.* Free of both problems. Rejected
  because it abandons the modifier the user actually reaches for, and on
  Windows/Linux it collides with the devtools trio anyway.

**The trade-off.** Two chords instead of one, and the sidebar's chord is one key
wider than the request. Paid for by making the badge render `⇧B` rather than
`B`, so nobody has to remember which surface wants Shift — the label is the
whole instruction.

**Why.** Simplicity here means *the affordance never lies*. A uniform rule that
misfires on three digits or eats a tab is not simpler for the person using it,
it is just simpler to describe. Rows get the plain modifier because rows are the
frequent motion and the one the request centred on.

## 2. Hold-to-reveal drives the rendering; nothing enters a mode

Label visibility is a pure function of "is the modifier down right now",
maintained by `keydown`/`keyup`/`blur` on `window`. There is no hint mode to
enter, no state to escape, and no cleanup after a jump — keeping the modifier
held after `⌘1` simply labels the page you land on.

**The alternative.** A toggled mode (a key that turns hints on until dismissed),
which is how Vimium-style link hints work and which survives the fact that some
platforms drop `keyup`.

**The trade-off.** Modifier state can genuinely be lost — `⌘Tab` away and no
`keyup` ever arrives — so hints can strand visible. Handled by clearing on
window `blur`, which is a guess about *why* focus left rather than a fact.

**Why.** A mode is a thing to remember you are in. The whole value of this
feature is that it costs nothing to discover and nothing to leave; `⌘K` already
covers the case where you want a persistent, searchable surface.

## 3. Ten rows get hints, and the rest get nothing

`1`–`9` then `0`. The default page size is 20, so on a full page the bottom half
carries no labels at all.

**The alternatives.** Two-key sequences (`aa`, `ab`, …) to cover any page size,
or letters after the digits (`1`…`0`, then `q`, `w`, `e`).

**The trade-off.** On a 20- or 50-row page the feature visibly stops halfway,
which reads as unfinished until you know it is deliberate.

**Why.** The value is "one keystroke, no thinking". A second keystroke to
disambiguate is `j`/`k`/`o` with extra steps, and mixed digit/letter alphabets
make the user parse the label before pressing it. Past ten rows, `⌘K` and the
search box are genuinely the better tools, and both are one keystroke away.

## 4. Letters are assigned from the live sidebar, not pinned per route

`assignNavHintLetters` walks the rendered sidebar in order and picks each item's
letter from its label, skipping a reserved set. It runs against the nav as
displayed, so Observability — which only appears when the manager reports o11y
enabled — participates like any other item.

**The alternative.** A letter hard-coded on each `SidebarItem`, so a route's
hint never changes.

**The trade-off.** Letters are positional, so they can shift. Enabling
observability, or adding a nav item above an existing one, can hand a different
letter to an item whose letter a user had already learned.

**Why.** Extensibility: a hard-coded table is a second place to update every
time the nav changes, and the failure mode when someone forgets is a duplicate
letter that silently shadows another item. The assignment function cannot
produce a collision. Worth revisiting if the sidebar stabilizes and muscle
memory starts mattering more than correctness.

## 5. `Jobs` is `⇧B`, and mnemonics lose to a reserved-letter list

Assignment skips `c i j n q t w` — tab and window lifecycle, macOS log-out, and
the devtools trio — because those are the combinations a browser will not hand
back. `Jobs` therefore falls through `j` (reserved) and `o` (taken by Overview)
to `b`.

**The alternative.** Take the obvious mnemonic anyway. `⌘⇧J` does nothing in
Chrome on macOS, so `Jobs → J` would work on the machine this was built on.

**The trade-off.** The three most-used items have non-obvious letters (`Jobs`
`⇧B`, `Workers` `⇧K`, `KV Store` `⇧D`), so nobody will guess them cold.

**Why.** `Ctrl⇧J` opens the devtools console on Windows and Linux, and a
shortcut whose behavior depends on the operator's OS is worse than an arbitrary
one. Nobody needs to guess: the letter is on screen the moment you reach for the
modifier, which is the entire premise of the feature.

## 6. Only the page's primary index registers digit hints

`useIndexJumpTargets` is a single global registration with an owner token, not a
per-table one. On Volumes — which renders a volumes table *and* a drivers table
— only the volumes table registers, so the drivers rows show no digits.

**The alternative.** Per-table registration with a table-scoped digit space,
disambiguated by which table has focus or is hovered.

**The trade-off.** A page's secondary tables get no hints, and a page that
registered the wrong table would be quietly wrong.

**Why.** Simplicity: "the digits belong to the thing this page is an index of"
needs no rule about focus, and there is exactly one sensible answer on every
page we have. Per-table scoping introduces a question ("which table am I aiming
at?") that hold-to-reveal was supposed to eliminate.

## 7. Chords are matched on `event.code`, not `event.key`

`⌘⇧B` arrives with `event.key === 'B'` on a US layout and something else
entirely on others; with Shift held, digit keys report punctuation. Matching
`KeyB`/`Digit1` uses the physical key, with `event.key` as a fallback for
layouts and IMEs that leave `code` empty.

**The alternative.** Match `event.key`, lowercased, as the existing `j`/`k`/`o`
handlers already do.

**The trade-off.** On a non-QWERTY layout the physical key no longer matches the
letter printed in the badge — a Dvorak user pressing the key labelled `B` is
pressing `KeyX`.

**Why.** `event.key` is unusable here: Shift mangles digits into `!@#`, so a
`key`-based match would need a per-layout punctuation table to read `⇧1`. The
badge/physical-key mismatch affects alternative-layout users only, and only for
the sidebar letters; the fallback keeps the feature working rather than failing
closed.

## 8. Collapsed sidebar hides the letters but keeps answering the chords

The request said the sidebar hints are for the expanded sidebar. Collapsed to
icons there is no room for a badge, so none is drawn — but `⌘⇧E` still opens
Events.

**The alternative.** Disable the chords whenever the label is not visible, so
the two always agree.

**The trade-off.** A collapsed-sidebar user has working shortcuts with no way to
discover or re-check them in the UI. They are documented in
`docs/ui-architecture.md`, which is a weaker teacher than the badge.

**Why.** Someone who collapsed the sidebar did it to reclaim space, not to give
up navigation, and they are exactly the user most likely to have learned the
letters already. Silently revoking a shortcut because a panel is narrow would be
the more surprising behavior.
