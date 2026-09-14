# Parchment, Olive + Citron

AIDA uses one visual theme across the website, account flow and analytics
workspace. Light parchment, fresh olive and citron surfaces pair with forest-green
accents, confident headings and responsive motion. Query and dashboard behavior
remain independent of these presentation styles.

## Shared palette and typography

| Token | Color | Use |
| --- | --- | --- |
| `--parchment` | `#FBFAF4` | Page background and browser theme color |
| `--surface` | `#FFFFFF` | Forms, panels and working surfaces |
| `--olive` | `#526B23` | Primary actions, selected controls and chart accents |
| `--olive-dark` | `#183C2E` | Forest-green accents and strong brand color |
| `--ink` | `#19372B` | Main text |
| `--muted` | `#626D5C` | Secondary text |
| `--line` | `#DCE3D2` | Dividers and borders |
| `--sage` | `#B9D393` | Supporting chart and accent color |
| `--hero-green` | `#EDF3CD` | Light olive hero background |
| `--olive-mid` | `#667F2B` | Supporting olive accent |
| `--olive-light` | `#E1EF87` | Citron highlights and selected controls |

Use **Manrope** (`--font-ui`) for controls, tables, navigation, body text and primary
headings. Headings use weight 650 for stronger hierarchy. **Newsreader**
(`--font-display`) supplies occasional italic accents and selected headline totals.
Other scripts fall back to system fonts.

The hero pairs a bold Manrope heading with a Newsreader italic accent at weight
450. A light olive-to-citron background uses sparse CSS dots and a large outline
ring behind the white demo panel. It has no image download or grain overlay.
Decoration accepts no pointer events. Dark forest text and controls contrast with
the light canvas; the console header reverses to citron on forest green.

## Motion and interaction

Hero copy enters in a short stagger. Sections and cards enter once as they scroll
into view. Existing demo stages, definition rows, chart bars, security details and
privacy choices transition when selected. Buttons, arrows, navigation, forms and
product panels provide short hover, focus, press and arrival feedback. A thin
navigation progress line follows scroll position without rerendering the page.

The walkthrough has an explicit pause/play button. Its typing and autoplay pause
when less than 15% of the demo is visible, when the document is hidden or when the
visitor pauses it. Manual stage selection stays selected until play or another
example is chosen. A paused walkthrough stays paused when examples are changed.

`prefers-reduced-motion` disables autoplay, typing and visual animations, including
when the preference changes during a visit. Examples and tabs remain interactive.
Server-rendered content starts visible with a complete question; scroll effects
never leave content hidden if JavaScript is unavailable. Motion uses CSS and native
Web Animations, with no animation dependency or model calls.

Both font families are self-hosted WOFF2 files with pinned Fontsource 5.3.0
provenance, SHA-256 checksums and SIL Open Font License 1.1 files in
[`frontend/public/fonts`](../frontend/public/fonts/README.md). There is no font
CDN request or build-time font download. `font-display: swap` keeps text visible
while fonts load; the normal Latin faces are preloaded. Keep the license files
when distributing the application.

## Implementation ownership

- [`fonts.css`](../frontend/app/fonts.css) declares the font faces and type variables.
- [`globals.css`](../frontend/app/globals.css) and [`aida.css`](../frontend/app/aida.css)
  provide base component styles, layout mechanics and responsive behavior.
- [`theme.css`](../frontend/app/theme.css) is the shared palette and visual hierarchy
  layer. It loads after the base CSS. Put shared theme adjustments here and keep
  route-specific colors from drifting away from the palette.
- [`motion.css`](../frontend/app/motion.css) owns landing interactions; the page
  coordinates viewport, visibility and motion preferences.
- [`product-motion.css`](../frontend/app/product-motion.css) owns workspace,
  account, onboarding and benchmark transitions. Both load after the theme.
- [`BrandLogo`](../frontend/components/BrandLogo.tsx) renders the architectural A
  with a folded ledger crossbar and a wordmark. It inherits `currentColor`, accepts
  `className` and `compact`, and leaves navigation to its parent link. Its CSS
  module owns the signature proportions.
- [`icon.svg`](../frontend/app/icon.svg) uses the same symbol in citron on forest
  green. [`layout.tsx`](../frontend/app/layout.tsx) owns the global font imports,
  preloads, page metadata and parchment browser theme color.

The theme covers `/`, `/benchmarks`, `/signup`, `/login`, `/waitlist`,
`/onboarding` and `/workspace`, including the interactive landing demo, query
builder, charts, tables, dashboards, catalog and database connection controls.
Shared focus styles and reduced-motion rules apply across these pages.
See [frontend verification](THEME-VERIFICATION.md) for the browser journeys and
visual review procedure.

## Registration and availability

Navigation uses **Sign up** and **Sign in**. When the authenticated backend is
available, sign-up creates a real account and continues to onboarding; sign-in
opens onboarding or the workspace according to the account's state.

With `NEXT_PUBLIC_AIDA_MODE=preview`, or while the backend is unavailable, the
registration form collects a name, email and explicit email consent. Optional
team details can be expanded. This form creates an availability registration,
not an authenticated account, and it never accepts or forwards a password.

The form submits to `/api/interest` and proceeds only when the response is
successful **and `stored === true`**. It then opens `/waitlist`, explaining that
workspaces open in stages and account setup follows availability. Failed or
unconfigured storage keeps the form visible with an error and preserves the
entered details. Direct visits to `/waitlist` without a confirmation marker show
a sign-up link rather than a saved-registration claim. The browser stores only
the registration source marker, with no name or email; if session storage is
unavailable, a successful submission shows the confirmation inline.

Configure the Supabase interest table or an interest webhook before publishing
registration. The paused sign-in flow states that sign-in is unavailable and
offers availability updates. See [deployment setup](DEPLOYMENT.md) for the
storage environment variables and online account configuration.

## Production assets

The Docker runtime copies `public/`, including the fonts and their licenses,
alongside the standalone Next.js bundle. `npm start` also copies these assets
into the standalone server directory. Font URLs use the same origin as AIDA and
are compatible with the existing `font-src 'self' data:` policy.
