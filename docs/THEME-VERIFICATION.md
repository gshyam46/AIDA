# Parchment & Olive frontend verification

This change covers the landing page, workspace, dashboards, data catalog, charts,
benchmarks, account screens, onboarding, registration confirmation, logo and favicon.
It does not change query interpretation, database execution or model configuration.
See [the theme guide](THEME.md) for the design tokens, fonts and asset ownership.

## Verified results

The production build, including TypeScript, passed. The browser suites passed
**50 checks**: 25 public-page theme checks, 14 account/product journey checks and
11 availability checks. See the [recorded results](evidence/theme-ui.json).

Chart switching preserved the results, and saved dashboard refresh/reopen used
the validated plan with zero model calls. KPI values fit their cards at 768, 390
and 320 pixels; signup and sign-in links remain visible at 320 pixels. Desktop and
mobile screenshots were inspected after correcting contrast and narrow layouts.

## Reproduce

Build and start the frontend with a running local AIDA backend that requires account
authentication. Then, from the repository root, run:

```powershell
Set-Location frontend
npm.cmd run build
Set-Location ..
node scripts/e2e-theme.cjs
node scripts/e2e-availability.cjs
node scripts/e2e-auth.cjs
```

The scripts use Playwright Chromium or installed Microsoft Edge. Override the base
URL with `AIDA_BASE_URL`. Browser screenshots are written to the ignored
`artifacts/e2e-theme`, `artifacts/e2e-availability` and `artifacts/e2e-auth`
directories. The theme and account suites also write JSON reports; the availability
suite prints each result to the terminal.

The theme suite checks the landing, signup, login and benchmark pages at 320, 390,
768, 1024 and 1440 pixels; all demo stages and examples; keyboard activation of the
security controls; the privacy toggle; local font loading; the favicon; page
overflow and browser errors.

The availability suite simulates unavailable services and interest-storage
responses in the browser. It checks successful registration, confirmation reload,
failed storage with preserved input, missing confirmation, and the transition from
account signup to registration without transferring a password. These checks do
not send registration emails or write to a hosted waitlist.

The account suite creates a synthetic local account, completes onboarding with a
private logistics sample, inspects results, exercises the product navigation, and
signs out and back in. Its default run uses existing validated plans and makes no
model calls. Real question evaluation is separate from this frontend change.

## Visual review

Review the desktop landing and workspace, mobile landing and signup, the benchmark
page and registration confirmation screenshots. Check serif headings and headline
totals, readable UI labels, olive selected states, warm chart series and the same
logo across routes. Local font files also ship in the standalone/Docker runtime.

The signup and sign-in entry points remain visible on mobile. Preview signup uses
name/email registration and opens the waitlist confirmation after a successful
storage response. An available backend retains the regular account and onboarding
flow.
