# Parchment + Olive

AIDA uses one visual theme across the website, account flow and analytics
workspace. Warm surfaces, restrained olive accents, editorial headings and clear
interface typography provide hierarchy without changing the query or dashboard
workflow.

## Shared palette and typography

| Token | Color | Use |
| --- | --- | --- |
| `--parchment` | `#F3EEDF` | Page background and browser theme color |
| `--surface` | `#FAF7EF` | Forms, panels and working surfaces |
| `--olive` | `#555D38` | Primary actions, selected controls and chart accents |
| `--olive-dark` | `#303724` | Dark sections and strong brand color |
| `--ink` | `#2B3024` | Main text |
| `--muted` | `#6B6D5C` | Secondary text |
| `--line` | `#D8D3C3` | Dividers and borders |
| `--sage` | `#B5BA94` | Supporting chart and accent color |
| `--hero-green` | `#4F6A34` | Fresh green at the start of the hero background |
| `--olive-mid` | `#557236` | Middle shade of the hero background |
| `--olive-light` | `#5D773C` | Brighter olive behind the parchment demo |

Use **Manrope** (`--font-ui`) for controls, tables, navigation and body text.
Use **Newsreader** (`--font-display`) for prominent headings; its real italic
provides occasional editorial emphasis. Dense data and table values stay in the UI
font; headline totals may use Newsreader. Other scripts fall back to system fonts.

The hero uses a larger Newsreader headline at weight 400, with real italic
emphasis and a Manrope lead at weight 500. Its fresh green-to-olive
background combines faint hatching with static engraved curves from
`public/hero-contours.svg`. Decoration stays behind the content and accepts no
pointer events. On mobile, the curves sit lower to keep the headline clear. The
light demo panel and fresh sage call to action provide contrast against the green.
Small hero text stays white and the curves use 4% opacity to keep labels readable
over the brighter end of the background.

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
- [`BrandLogo`](../frontend/components/BrandLogo.tsx) renders the architectural A
  with a folded ledger crossbar and a wordmark. It inherits `currentColor`, accepts
  `className` and `compact`, and leaves navigation to its parent link. Its CSS
  module owns the signature proportions.
- [`icon.svg`](../frontend/app/icon.svg) uses the same symbol in parchment on deep
  olive. [`layout.tsx`](../frontend/app/layout.tsx) owns the global font imports,
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
